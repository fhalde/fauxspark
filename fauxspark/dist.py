import numpy as np
from typing import Any


def uniform(_: Any, n: int) -> np.ndarray:
    w = np.ones(n)
    w /= w.sum()
    return w


def zipf(dist: dict[Any, Any], n: int) -> np.ndarray:
    alpha = dist["alpha"]
    w = np.random.zipf(alpha, n)
    w /= w.sum()
    return w


def normal(dist: dict[Any, Any], n: int) -> np.ndarray:
    mu = dist["loc"]
    sigma = dist["scale"]
    w = np.random.normal(mu, sigma, n)
    w /= w.sum()
    return w


def pareto(dist: dict[Any, Any], n: int) -> np.ndarray:
    alpha = dist["alpha"]
    w = np.random.pareto(alpha, n)
    w /= w.sum()
    return w


def exponential(dist: dict[Any, Any], n: int) -> np.ndarray:
    scale = dist["scale"]
    w = np.random.exponential(scale, n)
    w /= w.sum()
    return w


def weights(dist: dict[Any, Any], n: int) -> np.ndarray:
    kind = dist["kind"]
    func = globals().get(kind)
    if func is None:
        raise ValueError(f"Unknown distribution kind: {kind}")
    return func(dist, n)
