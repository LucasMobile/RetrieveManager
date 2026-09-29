"""JPEG 2000 encoding for compaction (pydicom + pylibjpeg-openjpeg).

Runs inside the isolated codec worker (app/codec_worker.py): a crash in the
native encoder must never take the worker process down with it.

Profiles:
  lossless - always JPEG 2000 Lossless.
  lossy    - by BitsStored: <=8 bits cr=10, 9-12 bits cr=5; anything else
             (or an encoder failure) falls back to lossless.
Objects without pixel data (SR, PDF, KOS...), pixel data that already arrived
compressed and very large or tiny images are copied without re-encoding, as
are images the encoder cannot handle.
The SOP Instance UID is never changed.
"""

from __future__ import annotations

from dataclasses import dataclass

from pydicom.dataset import Dataset
from pydicom.multival import MultiValue
from pydicom.uid import (
    JPEG2000,
    ExplicitVRLittleEndian,
    ImplicitVRLittleEndian,
    JPEG2000Lossless,
)

PROFILE_LOSSLESS = "lossless"
PROFILE_LOSSY = "lossy"
PROFILES = (PROFILE_LOSSLESS, PROFILE_LOSSY)

METHOD_LOSSLESS = "lossless"
METHOD_LOSSY = "lossy"
METHOD_COPY = "copy"

LOSSY_METHOD_J2K = "ISO_15444_1"

NON_COMPRESSIBLE_SOP_CLASS_UIDS = frozenset(
    {
        "1.2.840.10008.5.1.4.1.1.104.1",  # Encapsulated PDF
        "1.2.840.10008.5.1.4.1.1.104.2",  # Encapsulated CDA
        "1.2.840.10008.5.1.4.1.1.104.3",  # Encapsulated STL
        "1.2.840.10008.5.1.4.1.1.104.4",  # Encapsulated OBJ
        "1.2.840.10008.5.1.4.1.1.104.5",  # Encapsulated MTL
        "1.2.840.10008.5.1.4.1.1.88.11",  # Basic Text SR
        "1.2.840.10008.5.1.4.1.1.88.22",  # Enhanced SR
        "1.2.840.10008.5.1.4.1.1.88.33",  # Comprehensive SR
        "1.2.840.10008.5.1.4.1.1.88.34",  # Comprehensive 3D SR
        "1.2.840.10008.5.1.4.1.1.88.40",  # Procedure Log
        "1.2.840.10008.5.1.4.1.1.88.50",  # Mammography CAD SR
        "1.2.840.10008.5.1.4.1.1.88.59",  # Key Object Selection
    }
)

# Only native little-endian pixel data is re-encoded; everything else is kept
# exactly as it arrived.
_ENCODABLE_SYNTAXES = frozenset({ExplicitVRLittleEndian, ImplicitVRLittleEndian})

# Attributes the encoder must never change.
_INVARIANT_KEYWORDS = (
    "SOPInstanceUID",
    "Rows",
    "Columns",
    "NumberOfFrames",
    "SamplesPerPixel",
    "BitsAllocated",
    "BitsStored",
)


class CodecInvariantError(RuntimeError):
    """The encoded dataset no longer describes the same image."""


@dataclass(frozen=True)
class EncodeOutcome:
    method: str
    reason: str = ""
    ratio: float | None = None


def lossy_ratio(ds: Dataset) -> float | None:
    """Target compression ratio for the lossy profile, or None for lossless."""
    samples = int(ds.get("SamplesPerPixel", 1) or 1)
    photometric = str(ds.get("PhotometricInterpretation", "") or "").upper()
    planar = ds.get("PlanarConfiguration", None)
    eligible = (samples == 1 and photometric in {"MONOCHROME1", "MONOCHROME2"}) or (
        samples == 3
        and photometric in {"RGB", "YBR_FULL", "YBR_ICT"}
        and planar in (0, None)
    )
    if not eligible:
        return None
    bits = int(ds.get("BitsStored", 0) or 0)
    if 0 < bits <= 8:
        return 10.0
    if 8 < bits <= 12:
        return 5.0
    return None


def copy_reason(ds: Dataset, file_size: int, max_encode_bytes: int) -> str:
    """Why the object must be kept as received, or "" when it can be encoded."""
    sop_class = str(ds.get("SOPClassUID", "") or "").strip()
    if sop_class in NON_COMPRESSIBLE_SOP_CLASS_UIDS or "PixelData" not in ds:
        return "non_image"
    syntax = getattr(getattr(ds, "file_meta", None), "TransferSyntaxUID", None)
    if syntax not in _ENCODABLE_SYNTAXES:
        return "already_compressed"
    if file_size > max_encode_bytes:
        return "too_large"
    # openjpeg encodes 6 resolution levels: each side needs at least 32 pixels.
    if min(int(ds.get("Rows", 0) or 0), int(ds.get("Columns", 0) or 0)) < 32:
        return "too_small"
    return ""


def encode(ds: Dataset, profile: str) -> EncodeOutcome:
    """Encode the dataset's pixel data in place.

    When lossless encoding also fails the dataset is left untouched and the
    outcome is "copy": the image is delivered as received.
    """
    before = {keyword: ds.get(keyword) for keyword in _INVARIANT_KEYWORDS}
    raw_size = len(ds.PixelData)
    outcome: EncodeOutcome | None = None
    ratio = lossy_ratio(ds) if profile == PROFILE_LOSSY else None
    reason = "bits" if profile == PROFILE_LOSSY and ratio is None else ""
    if ratio is not None:
        try:
            # compress() only touches the dataset after every frame encoded.
            ds.compress(JPEG2000, j2k_cr=[ratio], generate_instance_uid=False)
        except Exception as exc:
            reason = f"lossy_failed:{type(exc).__name__}"
        else:
            actual = raw_size / max(1, len(ds.PixelData))
            _mark_lossy(ds, actual)
            outcome = EncodeOutcome(METHOD_LOSSY, ratio=round(actual, 2))
    if outcome is None:
        try:
            ds.compress(JPEG2000Lossless, generate_instance_uid=False)
        except Exception as exc:
            return EncodeOutcome(
                METHOD_COPY, reason=f"lossless_failed:{type(exc).__name__}"
            )
        outcome = EncodeOutcome(METHOD_LOSSLESS, reason=reason)
    after = {keyword: ds.get(keyword) for keyword in _INVARIANT_KEYWORDS}
    changed = [
        keyword for keyword in _INVARIANT_KEYWORDS if before[keyword] != after[keyword]
    ]
    if changed:
        raise CodecInvariantError("encoder changed " + ",".join(changed))
    return outcome


def _mark_lossy(ds: Dataset, ratio: float) -> None:
    """PS3.3 C.7.6.1.1.5: record that (and how) the image was lossy compressed.

    Ratio and method are histories: earlier lossy steps are kept.
    """
    ds.LossyImageCompression = "01"
    ratios = _as_list(ds.get("LossyImageCompressionRatio"))
    methods = _as_list(ds.get("LossyImageCompressionMethod"))
    ds.LossyImageCompressionRatio = [*ratios, f"{ratio:.2f}"[:16]]
    ds.LossyImageCompressionMethod = [*methods, LOSSY_METHOD_J2K]


def _as_list(value) -> list:
    if value is None or value == "":
        return []
    if isinstance(value, MultiValue | list | tuple):
        return list(value)
    return [value]
