"""Experiment design (10.1, 10.4): deterministic assignment, outcome rows, planted effect."""
from __future__ import annotations

import hashlib
from statistics import fmean, stdev

from experimentation.stats import sample_size


def assign(experiment: str, customer_id: str) -> str:
    digest = hashlib.sha256(f"{experiment}:{customer_id}".encode("utf-8")).digest()
    return "treatment" if digest[0] % 2 else "control"


def outcomes(units: dict, population: set, experiment: str, planted_lift: float) -> list[tuple]:
    """One (variant, revenue, converted) row per customer in the population.

    The planted lift multiplies treatment revenue. Customers absent from units had no
    published orders, so they count as zero revenue and no conversion.
    """
    rows = []
    for customer in sorted(population):
        revenue, orders = units.get(customer, (0.0, 0))
        variant = assign(experiment, customer)
        if variant == "treatment":
            revenue *= 1.0 + planted_lift
        rows.append((variant, revenue, orders > 0))
    return rows


def required_units_per_arm(control_revenues: list[float], planted_lift: float) -> int:
    mean = fmean(control_revenues)
    sd = stdev(control_revenues)
    return sample_size(delta=planted_lift * mean, sd=sd)