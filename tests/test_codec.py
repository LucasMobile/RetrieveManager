import unittest
from io import BytesIO
from unittest.mock import patch

import numpy as np
import pydicom
from pydicom.dataset import Dataset, FileMetaDataset
from pydicom.encaps import encapsulate
from pydicom.uid import (
    JPEG2000,
    CTImageStorage,
    ExplicitVRLittleEndian,
    ImplicitVRLittleEndian,
    JPEG2000Lossless,
    JPEGLosslessSV1,
    generate_uid,
)

from app.codec import (
    METHOD_COPY,
    METHOD_LOSSLESS,
    METHOD_LOSSY,
    PROFILE_LOSSLESS,
    PROFILE_LOSSY,
    copy_reason,
    encode,
    lossy_ratio,
)


def image(bits=12, size=64, photometric="MONOCHROME2", samples=1, syntax=None):
    ds = Dataset()
    ds.file_meta = FileMetaDataset()
    ds.file_meta.TransferSyntaxUID = syntax or ExplicitVRLittleEndian
    ds.SOPClassUID = CTImageStorage
    ds.SOPInstanceUID = generate_uid()
    ds.Rows = ds.Columns = size
    ds.SamplesPerPixel = samples
    ds.PhotometricInterpretation = photometric
    ds.BitsAllocated = 8 if bits <= 8 else 16
    ds.BitsStored = bits
    ds.HighBit = bits - 1
    ds.PixelRepresentation = 0
    if samples == 3:
        ds.PlanarConfiguration = 0
    rng = np.random.default_rng(7)
    dtype = np.uint8 if bits <= 8 else np.uint16
    shape = (size, size, samples) if samples > 1 else (size, size)
    ds.PixelData = rng.integers(0, 2**bits, shape, dtype=dtype).tobytes()
    return ds


def roundtrip(ds):
    buffer = BytesIO()
    ds.save_as(buffer, enforce_file_format=True)
    buffer.seek(0)
    return pydicom.dcmread(buffer)


class LossyRatioTest(unittest.TestCase):
    def test_bits_rule(self):
        self.assertEqual(lossy_ratio(image(bits=8)), 10.0)
        self.assertEqual(lossy_ratio(image(bits=10)), 5.0)
        self.assertEqual(lossy_ratio(image(bits=12)), 5.0)
        self.assertIsNone(lossy_ratio(image(bits=16)))

    def test_only_grayscale_and_true_color_are_eligible(self):
        self.assertEqual(lossy_ratio(image(bits=8, samples=3, photometric="RGB")), 10.0)
        self.assertIsNone(lossy_ratio(image(bits=8, photometric="PALETTE COLOR")))


class EncodeTest(unittest.TestCase):
    def test_lossy_keeps_uid_and_marks_the_image(self):
        ds = image(bits=12)
        uid = ds.SOPInstanceUID
        outcome = encode(ds, PROFILE_LOSSY)

        saved = roundtrip(ds)
        self.assertEqual(outcome.method, METHOD_LOSSY)
        self.assertEqual(saved.file_meta.TransferSyntaxUID, JPEG2000)
        self.assertEqual(saved.SOPInstanceUID, uid)
        self.assertEqual(saved.LossyImageCompression, "01")
        self.assertEqual(saved.LossyImageCompressionMethod, "ISO_15444_1")
        self.assertGreater(float(saved.LossyImageCompressionRatio), 1)
        self.assertEqual(saved.pixel_array.shape, (64, 64))

    def test_previous_lossy_history_is_kept(self):
        ds = image(bits=8)
        ds.LossyImageCompression = "01"
        ds.LossyImageCompressionRatio = "8"
        ds.LossyImageCompressionMethod = "ISO_10918_1"
        encode(ds, PROFILE_LOSSY)

        saved = roundtrip(ds)
        self.assertEqual(
            list(saved.LossyImageCompressionMethod), ["ISO_10918_1", "ISO_15444_1"]
        )
        self.assertEqual(len(saved.LossyImageCompressionRatio), 2)

    def test_more_than_12_bits_is_lossless_even_in_lossy_profile(self):
        ds = image(bits=16)
        original = bytes(ds.PixelData)
        outcome = encode(ds, PROFILE_LOSSY)

        saved = roundtrip(ds)
        self.assertEqual((outcome.method, outcome.reason), (METHOD_LOSSLESS, "bits"))
        self.assertEqual(saved.file_meta.TransferSyntaxUID, JPEG2000Lossless)
        self.assertNotIn("LossyImageCompression", saved)
        self.assertEqual(saved.pixel_array.tobytes(), original)

    def test_lossless_profile_is_bit_exact(self):
        for kwargs in ({"bits": 12}, {"bits": 8, "samples": 3, "photometric": "RGB"}):
            with self.subTest(**kwargs):
                ds = image(**kwargs)
                original = bytes(ds.PixelData)
                outcome = encode(ds, PROFILE_LOSSLESS)
                saved = roundtrip(ds)
                self.assertEqual(outcome.method, METHOD_LOSSLESS)
                self.assertEqual(saved.pixel_array.tobytes(), original)

    def test_implicit_vr_source_is_written_as_explicit_jpeg2000(self):
        ds = image(bits=12, syntax=ImplicitVRLittleEndian)
        encode(ds, PROFILE_LOSSLESS)
        saved = roundtrip(ds)
        self.assertEqual(saved.file_meta.TransferSyntaxUID, JPEG2000Lossless)
        self.assertEqual(saved.Rows, 64)

    def test_lossy_encoder_failure_falls_back_to_lossless(self):
        ds = image(bits=8)
        original = bytes(ds.PixelData)
        real_compress = Dataset.compress

        def fail_lossy(self, uid, *args, **kwargs):
            if uid == JPEG2000:
                raise RuntimeError("encoder failure")
            return real_compress(self, uid, *args, **kwargs)

        with patch.object(Dataset, "compress", fail_lossy):
            outcome = encode(ds, PROFILE_LOSSY)

        saved = roundtrip(ds)
        self.assertEqual(outcome.method, METHOD_LOSSLESS)
        self.assertTrue(outcome.reason.startswith("lossy_failed"))
        self.assertNotIn("LossyImageCompression", saved)
        self.assertEqual(saved.pixel_array.tobytes(), original)


class LosslessFailureTest(unittest.TestCase):
    def test_image_is_kept_as_received_when_lossless_fails(self):
        ds = image(bits=12)
        original = bytes(ds.PixelData)
        with patch.object(Dataset, "compress", side_effect=RuntimeError("encoder")):
            outcome = encode(ds, PROFILE_LOSSY)

        self.assertEqual(outcome.method, METHOD_COPY)
        self.assertEqual(outcome.reason, "lossless_failed:RuntimeError")
        self.assertEqual(ds.file_meta.TransferSyntaxUID, ExplicitVRLittleEndian)
        self.assertEqual(bytes(ds.PixelData), original)
        self.assertNotIn("LossyImageCompression", ds)


class CopyReasonTest(unittest.TestCase):
    def test_objects_kept_as_received(self):
        sr = Dataset()
        sr.SOPClassUID = "1.2.840.10008.5.1.4.1.1.88.22"
        self.assertEqual(copy_reason(sr, 100, 10**9), "non_image")

        compressed = image(syntax=JPEGLosslessSV1)
        compressed.PixelData = encapsulate([b"\xff\xd8frame\xff\xd9"])
        self.assertEqual(copy_reason(compressed, 100, 10**9), "already_compressed")

        self.assertEqual(copy_reason(image(), 2_000, 1_000), "too_large")
        self.assertEqual(copy_reason(image(size=16), 100, 10**9), "too_small")
        self.assertEqual(copy_reason(image(), 100, 10**9), "")


if __name__ == "__main__":
    unittest.main()
