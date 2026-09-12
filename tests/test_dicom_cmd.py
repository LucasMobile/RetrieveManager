import unittest
from subprocess import PIPE, STDOUT
from unittest.mock import patch

from app.dicom_tools import (
    dcmcjpeg_cmd,
    echoscu_cmd,
    findscu_cmd,
    movescu_cmd,
    prior_movescu_cmd,
    redact_dicom_output,
    storescp_cmd,
)


class DicomCmdTest(unittest.TestCase):
    def test_findscu_is_dcmtk(self):
        cmd = findscu_cmd(
            "/opt/dcmtk/bin/findscu",
            "mob",
            "srvpacsFIR",
            "10.20.0.31",
            2104,
            "9202604600211333",
            "19820226",
        )
        self.assertEqual(cmd[0], "/opt/dcmtk/bin/findscu")
        self.assertIn("-S", cmd)
        self.assertIn("-aet", cmd)
        self.assertIn("mob", cmd)
        self.assertIn("-aec", cmd)
        self.assertIn("srvpacsFIR", cmd)
        self.assertIn("0008,0050=9202604600211333", cmd)
        self.assertIn("0010,0030=19820226", cmd)
        self.assertIn("0018,0015=", cmd)
        self.assertEqual(cmd[-2:], ["10.20.0.31", "2104"])
        self.assertNotIn("--bind", cmd)

    def test_movescu_is_dcmtk_not_dcm4che(self):
        cmd = movescu_cmd(
            "/opt/dcmtk/bin/movescu",
            "mob",
            "srvpacsFIR",
            "10.20.0.31",
            2104,
            "1.2.840.1",
        )
        self.assertEqual(cmd[0], "/opt/dcmtk/bin/movescu")
        self.assertIn("-S", cmd)
        self.assertIn("-aet", cmd)
        self.assertIn("mob", cmd)
        self.assertIn("-aec", cmd)
        self.assertIn("srvpacsFIR", cmd)
        self.assertIn("0020,000D=1.2.840.1", cmd)
        self.assertEqual(cmd[-2:], ["10.20.0.31", "2104"])
        self.assertNotIn("-aem", cmd)
        self.assertNotIn("--bind", cmd)
        self.assertNotIn("--dest", cmd)
        self.assertNotIn("-c", cmd)
        self.assertNotIn("+P", cmd)
        self.assertNotIn("--port", cmd)

    def test_echoscu_uses_configured_association(self):
        cmd = echoscu_cmd(
            "/opt/dcmtk/bin/echoscu", "mob", "srvpacsFIR", "10.20.0.31", 2104
        )
        self.assertEqual(cmd[0], "/opt/dcmtk/bin/echoscu")
        self.assertEqual(cmd[-2:], ["10.20.0.31", "2104"])
        self.assertIn("-aet", cmd)
        self.assertIn("mob", cmd)
        self.assertIn("-aec", cmd)
        self.assertIn("srvpacsFIR", cmd)

    def test_prior_move_uses_series_filters(self):
        cmd = prior_movescu_cmd(
            "/opt/dcmtk/bin/movescu",
            "mob",
            "srvpacsFIR",
            "10.20.0.31",
            2104,
            "ABDOMEN",
            "MR",
            "30211738",
            "19691027",
            "20230911-20260910",
        )
        self.assertIn("0008,0052=SERIES", cmd)
        self.assertIn("0018,0015=ABDOMEN", cmd)
        self.assertIn("0008,0060=MR", cmd)
        self.assertIn("0010,0020=30211738*", cmd)
        self.assertIn("0010,0030=19691027", cmd)
        self.assertIn("0008,0020=20230911-20260910", cmd)
        self.assertEqual(cmd[-2:], ["10.20.0.31", "2104"])

    def test_storescp_and_jpeg_unchanged(self):
        scp = storescp_cmd("/opt/dcmtk/bin/storescp", "mob", 444, "/data/in")
        self.assertIn("+xa", scp)
        self.assertIn("--fork", scp)
        jpeg = dcmcjpeg_cmd("/opt/dcmtk/bin/dcmcjpeg", "+e1", "a.dcm", "b.dcm")
        self.assertEqual(jpeg[1:4], ["-q", "+un", "+e1"])

    @patch("app.dicom_tools.subprocess.Popen")
    @patch("app.dicom_tools._require", return_value="/opt/dcmtk/bin/storescp")
    def test_storescp_output_is_captured_for_diagnostics(self, _require, popen):
        from app.dicom_tools import start_storescp

        start_storescp("RETRIEVE", 444, ".")

        kwargs = popen.call_args.kwargs
        self.assertEqual(kwargs["stdout"], PIPE)
        self.assertEqual(kwargs["stderr"], STDOUT)
        self.assertTrue(kwargs["text"])

    def test_dicom_output_redacts_phi(self):
        output = "(0010,0010) PN [SILVA^JOAO] # PatientName\nstatus ok"
        safe = redact_dicom_output(output)
        self.assertNotIn("SILVA", safe)
        self.assertIn("status ok", safe)


if __name__ == "__main__":
    unittest.main()
