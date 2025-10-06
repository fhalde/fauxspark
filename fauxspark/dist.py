import numpy as np
from pydantic import BaseModel, ConfigDict, field_validator
from typing import Any

from fauxspark.models import Input


def uniform(_, n: int) -> np.ndarray:
    w = np.ones(n).astype(np.float64)
    w /= w.sum()
    return w


def zipf(dist: dict, n: int) -> np.ndarray:
    alpha = dist["alpha"]
    w = np.random.zipf(alpha, n).astype(np.float64)
    w /= w.sum()
    return w


def normal(dist: dict, n: int) -> np.ndarray:
    mu = dist["loc"]
    sigma = dist["scale"]
    w = np.random.normal(mu, sigma, n).astype(np.float64)
    w /= w.sum()
    return w


def pareto(dist: dict, n: int) -> np.ndarray:
    alpha = dist["alpha"]
    w = np.random.pareto(alpha, n).astype(np.float64)
    w /= w.sum()
    return w


def exponential(dist: dict, n: int) -> np.ndarray:
    scale = dist["scale"]
    w = np.random.exponential(scale, n).astype(np.float64)
    w /= w.sum()
    return w


def weights(dist: dict, n: int) -> np.ndarray:
    kind = dist["kind"]
    func = globals().get(kind)
    if func is None:
        raise ValueError(f"Unknown distribution kind: {kind}")
    return func(dist, n)
