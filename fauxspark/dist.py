import numpy as np
import simpy


def uniform(_, n: int) -> list[float]:
    w = np.ones(n)
    w /= w.sum()
    return w


def zipf(dist: dict, n: int) -> list[float]:
    alpha = dist["alpha"]
    w = np.random.zipf(alpha, n).astype(float)
    w /= w.sum()
    return w


def normal(dist: dict, n: int) -> list[float]:
    mu = dist["loc"]
    sigma = dist["scale"]
    w = np.random.normal(mu, sigma, n).astype(float)
    w /= w.sum()
    return w


def pareto(dist: dict, n: int) -> list[float]:
    alpha = dist["alpha"]
    w = np.random.pareto(alpha, n).astype(float)
    w /= w.sum()
    return w


def exponential(dist: dict, n: int) -> list[float]:
    scale = dist["scale"]
    w = np.random.exponential(scale, n)
    w /= w.sum()
    return w


def weights(dist: dict, n: int) -> list[float]:
    kind = dist["kind"]
    func = globals().get(kind)
    if func is None:
        raise ValueError(f"Unknown distribution kind: {kind}")
    return func(dist, n)


def net():
    from ns.flow.cc import TCPReno
    from ns.flow.cubic import TCPCubic
    from ns.flow.flow import AppType, Flow
    from ns.packet.tcp_generator import TCPPacketGenerator
    from ns.packet.tcp_sink import TCPSink
    from ns.port.wire import Wire
    from ns.switch.switch import SimplePacketSwitch

    env = simpy.Environment()
