class Config:
    LOOKBACK_YEAR = 3
    START_YEAR = 2021
    END_YEAR_EXCLUDE = 2025
    END_YEAR_INCLUDE = 2024

    POSITIONS = ["G", "F", "C"]
    POSITION_DICT = {
        "G" : "Guards",
        "F" : "Forwards",
        "C" : "Centers"
    }

    META = {"player_name", "position", "season_year", "team_name", "player_id"}
    NON_STAT_COLS = list(META) + ['barthag_rank', 'min_pg', 'bpm', 'team_cluster', 'Cluster']

    TOP_PERCENT = 0.3
    BOTTOM_PERCENT = 0.4
    SAMPLE_SIZE_THRESHOLD = 30

    ESS_THRESHOLD = 30

    BPM_REPLACEMENT = -2