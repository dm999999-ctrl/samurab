#!/usr/bin/env python3
"""Poll OCI Singapore A1 capacity and launch exactly one approved scanner VM.

The execution path is deliberately deterministic:
- Poll OCI Compute Capacity Report.
- Treat only AVAILABLE as launchable.
- Launch only the exact approved A1 configuration.
- Refuse fallback shapes/sizes and refuse duplicate instances.
- Exit after successful verification.
"""

from __future__ import annotations

import logging
import os
import sys
import time
from dataclasses import dataclass

import oci

LOG = logging.getLogger("oracle-a1-agent")

TENANCY_OCID = "ocid1.tenancy.oc1..aaaaaaaaizwj5x5bxbi4xxlxu577gqkxlr2uu7ijmetbecexkrehi5ef3y2a"
REGION = "ap-singapore-1"
AVAILABILITY_DOMAIN = "EmQk:AP-SINGAPORE-1-AD-1"
COMPARTMENT_ID = TENANCY_OCID
SUBNET_ID = "ocid1.subnet.oc1.ap-singapore-1.aaaaaaaaesafpzvnjpdyjgjjge23c4vteqm2dhbprrcwtz55tbmnssn7jzea"
IMAGE_ID = "ocid1.image.oc1.ap-singapore-1.aaaaaaaap2qwxovwnfdsddsrlnmbcvlfan6fy52oohb5gjoe5ejmllinn73a"

SHAPE = "VM.Standard.A1.Flex"
OCPUS = 1
MEMORY_GB = 6.0
DISPLAY_NAME = "crypto-arbitrage-scanner"

POLL_SECONDS = max(1, int(os.getenv("POLL_SECONDS", "1")))
THROTTLE_BACKOFF_INITIAL = max(1, int(os.getenv("THROTTLE_BACKOFF_INITIAL", "2")))
THROTTLE_BACKOFF_MAX = max(THROTTLE_BACKOFF_INITIAL, int(os.getenv("THROTTLE_BACKOFF_MAX", "60")))
EXISTING_CHECK_INTERVAL = max(10, int(os.getenv("EXISTING_CHECK_INTERVAL", "30")))
RUN_WINDOW_SECONDS = max(60, int(os.getenv("RUN_WINDOW_SECONDS", "20700")))  # 5h45m
VERIFY_SECONDS = max(30, int(os.getenv("VERIFY_SECONDS", "180")))


@dataclass(frozen=True)
class ExactTarget:
    shape: str = SHAPE
    ocpus: int = OCPUS
    memory_gb: float = MEMORY_GB
    display_name: str = DISPLAY_NAME


TARGET = ExactTarget()


def build_config() -> oci.config.Config:
    required = ["OCI_USER_OCID", "OCI_FINGERPRINT", "OCI_PRIVATE_KEY"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing required GitHub secrets: {', '.join(missing)}")

    key = os.environ["OCI_PRIVATE_KEY"].replace("\\n", "\n")
    config = {
        "user": os.environ["OCI_USER_OCID"],
        "fingerprint": os.environ["OCI_FINGERPRINT"],
        "tenancy": TENANCY_OCID,
        "region": REGION,
        "key_content": key,
    }
    oci.config.validate_config(config)
    return config


def get_compute_client(config: oci.config.Config) -> oci.core.ComputeClient:
    client = oci.core.ComputeClient(config)
    client.base_client.signer.region = REGION
    return client


def get_network_client(config: oci.config.Config) -> oci.core.VirtualNetworkClient:
    return oci.core.VirtualNetworkClient(config)


def capacity_status(compute: oci.core.ComputeClient) -> str:
    details = oci.core.models.CreateComputeCapacityReportDetails(
        compartment_id=COMPARTMENT_ID,
        availability_domain=AVAILABILITY_DOMAIN,
        shape_availabilities=[
            oci.core.models.CreateCapacityReportShapeAvailabilityDetails(
                instance_shape=SHAPE,
                instance_shape_config=oci.core.models.CapacityReportInstanceShapeConfig(
                    ocpus=OCPUS,
                    memory_in_gbs=MEMORY_GB,
                ),
            )
        ],
    )
    report = compute.create_compute_capacity_report(
        create_compute_capacity_report_details=details,
    ).data

    status = report.shape_availabilities[0].availability_status
    LOG.info("A1 capacity: %s", status)
    return status


def list_existing_instances(compute: oci.core.ComputeClient) -> list[oci.core.models.Instance]:
    instances = []
    response = compute.list_instances(
        compartment_id=COMPARTMENT_ID,
        availability_domain=AVAILABILITY_DOMAIN,
        lifecycle_state="RUNNING",
    )
    instances.extend(response.data)
    while response.has_next_page:
        response = compute.list_instances(
            compartment_id=COMPARTMENT_ID,
            availability_domain=AVAILABILITY_DOMAIN,
            lifecycle_state="RUNNING",
            page=response.next_page,
        )
        instances.extend(response.data)

    return [i for i in instances if (i.display_name or "").strip() == DISPLAY_NAME]


def hard_safety_check() -> None:
    checks = {
        "region": REGION == "ap-singapore-1",
        "availability_domain": AVAILABILITY_DOMAIN == "EmQk:AP-SINGAPORE-1-AD-1",
        "shape": SHAPE == "VM.Standard.A1.Flex",
        "ocpus": OCPUS == 2,
        "memory_gb": MEMORY_GB == 12.0,
        "display_name": DISPLAY_NAME == "crypto-arbitrage-scanner",
    }
    failed = [name for name, ok in checks.items() if not ok]
    if failed:
        raise RuntimeError(f"Hard safety check failed: {', '.join(failed)}")


def launch_exact_instance(
    compute: oci.core.ComputeClient,
) -> oci.core.models.Instance:
    hard_safety_check()

    existing = list_existing_instances(compute)
    if existing:
        LOG.warning("Existing scanner VM already present (%s); will not create another.", existing[0].id)
        return existing[0]

    details = oci.core.models.LaunchInstanceDetails(
        availability_domain=AVAILABILITY_DOMAIN,
        compartment_id=COMPARTMENT_ID,
        display_name=DISPLAY_NAME,
        shape=SHAPE,
        shape_config=oci.core.models.LaunchInstanceShapeConfigDetails(
            ocpus=OCPUS,
            memory_in_gbs=MEMORY_GB,
        ),
        source_details=oci.core.models.InstanceSourceViaImageDetails(
            image_id=IMAGE_ID,
            source_type="image",
        ),
        create_vnic_details=oci.core.models.CreateVnicDetails(
            subnet_id=SUBNET_ID,
            assign_public_ip=True,
        ),
    )
    instance = compute.launch_instance(details).data
    LOG.info("Launch submitted: %s", instance.id)
    return instance


def verify_instance(
    compute: oci.core.ComputeClient,
    instance_id: str,
) -> bool:
    deadline = time.time() + VERIFY_SECONDS
    while time.time() < deadline:
        instance = compute.get_instance(instance_id).data
        LOG.info("Instance %s lifecycle=%s shape=%s ocpus=%s memory=%s",
                 instance.id, instance.lifecycle_state, instance.shape,
                 getattr(instance.shape_config, "ocpus", None),
                 getattr(instance.shape_config, "memory_in_gbs", None))

        exact = (
            instance.shape == SHAPE
            and float(instance.shape_config.ocpus) == float(OCPUS)
            and float(instance.shape_config.memory_in_gbs) == float(MEMORY_GB)
            and instance.display_name == DISPLAY_NAME
        )
        if not exact:
            raise RuntimeError("Launched instance failed exact target verification.")
        if instance.lifecycle_state == "RUNNING":
            return True
        if instance.lifecycle_state in {"TERMINATED", "TERMINATING"}:
            return False
        time.sleep(10)

    return False


def main() -> int:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)sZ %(levelname)s %(message)s",
    )

    config = build_config()
    compute = get_compute_client(config)
    # Construct once so credentials/SDK configuration are validated for networking too.
    _ = get_network_client(config)

    deadline = time.time() + RUN_WINDOW_SECONDS
    LOG.info("Starting Singapore A1 watcher. Normal poll=%ss; throttle backoff=%ss..%ss.", POLL_SECONDS, THROTTLE_BACKOFF_INITIAL, THROTTLE_BACKOFF_MAX)
    last_existing_check = 0.0
    backoff = THROTTLE_BACKOFF_INITIAL

    while time.time() < deadline:
        try:
            now = time.time()
            # Do not spend a second API call on every poll. Check for an existing VM
            # periodically, and always re-check immediately before any launch.
            if now - last_existing_check >= EXISTING_CHECK_INTERVAL:
                existing = list_existing_instances(compute)
                last_existing_check = now
                if existing:
                    LOG.info("Scanner VM already exists: %s. Stopping watcher.", existing[0].id)
                    return 0

            status = capacity_status(compute)
            backoff = THROTTLE_BACKOFF_INITIAL
            if status != "AVAILABLE":
                time.sleep(POLL_SECONDS)
                continue

            LOG.warning("A1 capacity AVAILABLE. Re-checking existing instances before launch.")
            existing = list_existing_instances(compute)
            if existing:
                LOG.info("Scanner VM already exists: %s. Stopping watcher.", existing[0].id)
                return 0

            LOG.warning("A1 capacity AVAILABLE and no scanner VM exists. Proceeding to deterministic safety-checked launch.")
            instance = launch_exact_instance(compute)
            ok = verify_instance(compute, instance.id)
            if not ok:
                raise RuntimeError("Launch did not reach RUNNING state before verification deadline.")
            LOG.info("Scanner VM successfully verified. Watcher complete.")
            return 0

        except oci.exceptions.ServiceError as exc:
            status = getattr(exc, "status", None)
            if status == 429 or status in {409, 503}:
                LOG.warning("OCI throttling/transient response %s: %s; backing off for %ss.", status, exc.message, backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, THROTTLE_BACKOFF_MAX)
            else:
                LOG.warning("OCI API error %s: %s; retrying in %ss.", status, exc.message, POLL_SECONDS)
                time.sleep(POLL_SECONDS)
        except Exception as exc:
            LOG.exception("Agent error: %s", exc)
            # Ambiguous or unexpected conditions are fail-closed.
            return 2

    LOG.info("Watcher window ended without capacity becoming AVAILABLE.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
