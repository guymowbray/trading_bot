from src.signal.signals import calculate_moving_averages, calculate_percent_away_from_ma

# Have pipelines for different signals which determine your stratergy.


SIGNAL_PIPELINE = [
    calculate_moving_averages,
    calculate_percent_away_from_ma,
]
