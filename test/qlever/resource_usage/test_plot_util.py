from qlever.resource_usage.plot_util import bands_from_durations


def test_bands_from_durations_lays_phases_back_to_back():
    bands = bands_from_durations(
        {"Load": 10.0, "Optimize": 5.0, "TOTAL time": 15.0}
    )
    assert bands == [("Load", 0.0, 10.0), ("Optimize", 10.0, 15.0)]
