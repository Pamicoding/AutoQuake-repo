import argparse
import json
from core.initializer import Initializer
from pathlib import Path

def parse_arguments():
    parser = argparse.ArgumentParser(description="AutoQuake Toolkit")
    parser.add_argument('--config', type=Path, required=True, help='Path to configuration JSON file')
    args = parser.parse_args()
    return args

def load_config(config_path):
    with config_path.open('r') as file:
        config = json.load(file)
    return config

def main():
    args = parse_arguments()
    config = load_config(args.config)

    required_keys = ["station_path", "name_of_eq_sequence", "analyze_range", "waveform_dir", "association_method"]
    for key in required_keys:
        if key not in config:
            raise ValueError(f"Missing required config parameter: {key}")
    # initialization
    initializer = Initializer(config)
    initializer.create_directory_structure()
    initializer.filter_single_equip()
    initializer.merge_waveform()
    parent_dir, station_path, date_list, mag_run, pz_dir, aso_method = initializer.get_materials()
    print("Initialization complete.")


if __name__ == "__main__":
    main()
