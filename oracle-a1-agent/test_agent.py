import importlib.util
from pathlib import Path

MODULE_PATH = Path(__file__).with_name("capacity_agent.py")
spec = importlib.util.spec_from_file_location("capacity_agent", MODULE_PATH)
module = importlib.util.module_from_spec(spec)
import sys
sys.modules[spec.name] = module
spec.loader.exec_module(module)


def test_exact_target_constants():
    assert module.REGION == "ap-singapore-1"
    assert module.AVAILABILITY_DOMAIN == "EmQk:AP-SINGAPORE-1-AD-1"
    assert module.SHAPE == "VM.Standard.A1.Flex"
    assert module.OCPUS == 1
    assert module.MEMORY_GB == 6.0
    assert module.DISPLAY_NAME == "crypto-arbitrage-scanner"

def test_capacity_report_models_exist():
    import oci

    assert hasattr(oci.core.models, "CreateComputeCapacityReportDetails")
    assert hasattr(oci.core.models, "CreateCapacityReportShapeAvailabilityDetails")
    assert hasattr(oci.core.models, "CapacityReportInstanceShapeConfig")


def test_hard_safety_check():
    module.hard_safety_check()


def test_fail_closed_when_target_is_changed():
    original = module.SHAPE
    try:
        module.SHAPE = "VM.Standard.E5.Flex"
        try:
            module.hard_safety_check()
        except RuntimeError:
            pass
        else:
            raise AssertionError("Safety check should reject fallback shape")
    finally:
        module.SHAPE = original
