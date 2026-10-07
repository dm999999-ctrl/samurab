#!/usr/bin/env python3
"""Poll OCI Singapore A1 capacity and launch exactly one approved scanner VM.

The execution path is deliberately deterministic:
- Poll OCI Compute Capacity Report.
- Treat only AVAILABLE as launchable.
- Launch only the exact approved A1 configuration.
- Refuse fallback shapes/sizes and refuse duplicate instances.
- Record every capacity response to capacity-history.csv.
- Exit after successful verification.
"""

from __future__ import annotations

import csv
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone

import oci

LOG = logging.getLogger("oracle-a1-agent")

TENANCY_OCID = "ocid1.tenancy.oc1..aaaaaaaaizwj5x5bxbi4xxlxu577gqkxlr2uu7ijmetbecexkrehi5ef3y2a"
REGION = "ap-singapore-1"
AVAILABILITY_DOMAIN = "EmQk:AP-SINGAPORE-1-AD-1"
COMPARTMENT_ID = TENANCY_OCID
SUBNET_ID = "ocid1.subnet.oc1.ap-singapore-1.aaaaaaaaesafpzvnjpdyjgjjge23c4vteqm2dhbprrcwtz55tbmnssn7jzea"
IMAGE_ID = "ocid1.image.oc1.ap-singapore-1.aaaaaaaap2qwxovwnfdsddsrlnmbcvlfan6fy52oohb5gjoe5ejmllinn73a"

SHAPE = "VM.Standard.A1.Flex"
OCPUS = 2
MEMORY_GB = 12.0
DISPLAY_NAME = "crypto-arbitrage-scanner"

POLL_SECONDS = max(1, int(os.getenv("POLL_SECONDS", "1")))
THROTTLE_BACKOFF_INITIAL = max(1, int(os.getenv("THROTTLE_BACKOFF_INITIAL", "2")))
THROTTLE_BACKOFF_MAX = max(THROTTLE_BACKOFF_INITIAL, int(os.getenv("THROTTLE_BACKOFF_MAX", "60")))
EXISTING_CHECK_INTERVAL = max(10, int(os.getenv("EXISTING_CHECK_INTERVAL", "30")))
RUN_WINDOW_SECONDS = max(60, int(os.getenv("RUN_WINDOW_SECONDS", "20700")))
VERIFY_SECONDS = max(30, int(os.getenv("VERIFY_SECONDS", "180")))
LAUNCH_RETRY_INITIAL = max(1, int(os.getenv("LAUNCH_RETRY_INITIAL", "2")))
LAUNCH_RETRY_MAX = max(LAUNCH_RETRY_INITIAL, int(os.getenv("LAUNCH_RETRY_MAX", "60")))
HISTORY_FILE = os.getenv("CAPACITY_HISTORY_FILE", "capacity-history.csv")


@dataclass(frozen=True)
class ExactTarget:
    shape: str = SHAPE
    ocpus: int = OCPUS
    memory_gb: float = MEMORY_GB
    display_name: str = DISPLAY_NAME


TARGET = ExactTarget()


def _history_writer():
    exists = os.path.exists(HISTORY_FILE) and os.path.getsize(HISTORY_FILE) > 0
    handle = open(HISTORY_FILE, "a", newline="", encoding="utf-8")
    writer = csv.writer(handle)
    if not exists:
        writer.writerow(["timestamp_utc", "status", "available_count", "latency_ms"])
        handle.flush()
    return handle, writer


def _record_capacity(writer, timestamp, status, available_count, latency_ms):
    writer.writerow([timestamp, status, available_count, latency_ms])
    writer.flush()


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


def capacity_status(compute: oci.core.ComputeClient, history_writer=None) -> str:
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
    started = time.perf_counter()
    report = compute.create_compute_capacity_report(
        create_compute_capacity_report_details=details,
    ).data
    latency_ms = round((time.perf_counter() - started) * 1000, 1)

    availability = report.shape_availabilities[0]
    status = availability.availability_status
    available_count = getattr(availability, "available_count", None)
    timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")

    if history_writer is not None:
        _record_capacity(history_writer, timestamp, status, available_count, latency_ms)

    if available_count is None:
        LOG.info("A1 capacity: %s (latency=%sms)", status, latency_ms)
    else:
        LOG.info("A1 capacity: %s available_count=%s (latency=%sms)", status, available_count, latency_ms)
    return status


def list_existing_instances(compute: oci.core.ComputeClient) -> list[oci.core.models.Instance]:
    """Return every non-terminated scanner VM, including provisioning states.

    This intentionally does not filter lifecycle_state=RUNNING: after an
    ambiguous launch response, a PROVISIONING/STARTING instance must block a
    second launch.
    """
    instances = []
    response = compute.list_instances(
        compartment_id=COMPARTMENT_ID,
        availability_domain=AVAILABILITY_DOMAIN,
    )
    instances.extend(response.data)
    while response.has_next_page:
        response = compute.list_instances(
            compartment_id=COMPARTMENT_ID,
            availability_domain=AVAILABILITY_DOMAIN,
            page=response.next_page,
        )
        instances.extend(response.data)

    return [
        i for i in instances
        if (i.display_name or "").strip() == DISPLAY_NAME
        and getattr(i, "lifecycle_state", None) != "TERMINATED"
    ]
def main() -> int:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)sZ %(levelname)s %(message)s",
    )

    config = build_config()
    compute = get_compute_client(config)
    _ = get_network_client(config)

    history_handle, history_writer = _history_writer()
    try:
        deadline = time.time() + RUN_WINDOW_SECONDS
        LOG.info(
            "Starting Singapore A1 watcher. Normal poll=%ss; throttle backoff=%ss..%ss; launch retry=%ss..%ss.",
            POLL_SECONDS, THROTTLE_BACKOFF_INITIAL, THROTTLE_BACKOFF_MAX,
            LAUNCH_RETRY_INITIAL, LAUNCH_RETRY_MAX,
        )
        last_existing_check = 0.0
        backoff = THROTTLE_BACKOFF_INITIAL
        launch_backoff = LAUNCH_RETRY_INITIAL

        while time.time() < deadline:
            try:
                now = time.time()
                if now - last_existing_check >= EXISTING_CHECK_INTERVAL:
                    existing = list_existing_instances(compute)
                    last_existing_check = now
                    if existing:
                        LOG.info("Scanner VM already exists: %s (%s). Stopping watcher.",
                                 existing[0].id, existing[0].lifecycle_state)
                        if existing[0].lifecycle_state == "RUNNING":
                            return 0
                        if verify_instance(compute, existing[0].id):
                            return 0
                        LOG.warning("Existing scanner VM did not verify as RUNNING; fail closed.")
                        return 2

                status = capacity_status(compute, history_writer)
                backoff = THROTTLE_BACKOFF_INITIAL
                if status != "AVAILABLE":
                    time.sleep(POLL_SECONDS)
                    continue

                LOG.warning("A1 capacity AVAILABLE. Re-checking existing instances before launch.")
                existing = list_existing_instances(compute)
                if existing:
                    LOG.info("Scanner VM already exists: %s (%s). Stopping watcher.",
                             existing[0].id, existing[0].lifecycle_state)
                    if existing[0].lifecycle_state == "RUNNING":
                        return 0
                    return 0 if verify_instance(compute, existing[0].id) else 2

                LOG.warning(
                    "A1 capacity AVAILABLE and no scanner VM exists. "
                    "Proceeding to deterministic safety-checked launch."
                )

                try:
                    instance = launch_exact_instance(compute)
                    launch_backoff = LAUNCH_RETRY_INITIAL
                except oci.exceptions.ServiceError as exc:
                    status_code = getattr(exc, "status", None)
                    LOG.warning(
                        "Launch API error %s: %s. Reconciling before any retry.",
                        status_code, getattr(exc, "message", str(exc)),
                    )
                    existing = list_existing_instances(compute)
                    if existing:
                        LOG.warning(
                            "Launch response was ambiguous but scanner VM %s exists in %s; "
                            "will not submit another launch.",
                            existing[0].id, existing[0].lifecycle_state,
                        )
                        return 0 if verify_instance(compute, existing[0].id) else 2

                    if status_code in {429, 409, 503}:
                        delay = min(launch_backoff, LAUNCH_RETRY_MAX)
                    else:
                        delay = min(launch_backoff, LAUNCH_RETRY_MAX)

                    LOG.warning(
                        "No scanner VM exists after launch failure. Re-checking capacity before retry in %ss.",
                        delay,
                    )
                    time.sleep(delay)
                    launch_backoff = min(launch_backoff * 2, LAUNCH_RETRY_MAX)
                    continue
                except Exception as exc:
                    LOG.exception(
                        "Unexpected launch failure: %s. Reconciling instance state before retry.",
                        exc,
                    )
                    existing = list_existing_instances(compute)
                    if existing:
                        LOG.warning(
                            "Scanner VM %s exists in %s after unexpected launch failure; "
                            "will not submit another launch.",
                            existing[0].id, existing[0].lifecycle_state,
                        )
                        return 0 if verify_instance(compute, existing[0].id) else 2

                    # The request did not leave a discoverable scanner VM. Before
                    # retrying, require capacity to be AVAILABLE again.
                    retry_status = capacity_status(compute, history_writer)
                    if retry_status != "AVAILABLE":
                        LOG.warning(
                            "Capacity is no longer AVAILABLE after launch failure (%s); "
                            "returning to normal polling.",
                            retry_status,
                        )
                        launch_backoff = LAUNCH_RETRY_INITIAL
                        time.sleep(POLL_SECONDS)
                        continue

                    delay = min(launch_backoff, LAUNCH_RETRY_MAX)
                    LOG.warning(
                        "No scanner VM exists and capacity remains AVAILABLE; "
                        "retrying launch in %ss.",
                        delay,
                    )
                    time.sleep(delay)
                    launch_backoff = min(launch_backoff * 2, LAUNCH_RETRY_MAX)
                    continue

                ok = verify_instance(compute, instance.id)
                if ok:
                    LOG.info("Scanner VM successfully verified. Watcher complete.")
                    return 0

                # Verification failure is also reconciled before any possible retry.
                existing = list_existing_instances(compute)
                if existing:
                    LOG.warning(
                        "Verification did not complete, but scanner VM %s still exists in %s; "
                        "will not launch another instance.",
                        existing[0].id, existing[0].lifecycle_state,
                    )
                    return 0 if existing[0].lifecycle_state != "TERMINATED" else 2

                retry_status = capacity_status(compute, history_writer)
                if retry_status == "AVAILABLE":
                    delay = min(launch_backoff, LAUNCH_RETRY_MAX)
                    LOG.warning(
                        "Launched VM disappeared before verification and capacity remains AVAILABLE; "
                        "retrying launch in %ss.",
                        delay,
                    )
                    time.sleep(delay)
                    launch_backoff = min(launch_backoff * 2, LAUNCH_RETRY_MAX)
                    continue

                LOG.warning("Verification failed and capacity is no longer AVAILABLE; returning to polling.")
                launch_backoff = LAUNCH_RETRY_INITIAL
                time.sleep(POLL_SECONDS)

            except oci.exceptions.ServiceError as exc:
                status = getattr(exc, "status", None)
                timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
                _record_capacity(history_writer, timestamp, f"ERROR_{status}", None, None)
                if status == 429 or status in {409, 503}:
                    LOG.warning(
                        "OCI throttling/transient response %s: %s; backing off for %ss.",
                        status, exc.message, backoff,
                    )
                    time.sleep(backoff)
                    backoff = min(backoff * 2, THROTTLE_BACKOFF_MAX)
                else:
                    LOG.warning(
                        "OCI API error %s: %s; retrying in %ss.",
                        status, exc.message, POLL_SECONDS,
                    )
                    time.sleep(POLL_SECONDS)
            except Exception as exc:
                LOG.exception("Agent error: %s", exc)
                return 2

        LOG.info("Watcher window ended without capacity becoming AVAILABLE.")
        return 0
    finally:
        history_handle.close()

