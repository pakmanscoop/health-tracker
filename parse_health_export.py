#!/usr/bin/env python3
"""Parse Apple Health export.xml into categorized, yearly CSV files.

Streams the XML to handle large files (600MB+) without loading into memory.
Organizes output into category folders (activity, heart, body, sleep, etc.)
with one CSV per type per year. Source names are shortened to save tokens.

Usage:
    python parse_health_export.py export.xml
    python parse_health_export.py export.xml --days 90 --output-dir ./data
    python parse_health_export.py export.xml --types StepCount,HeartRate --verbose
"""

import argparse
import csv
import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from xml.etree.ElementTree import iterparse


# Prefixes to strip from Apple Health type identifiers
TYPE_PREFIXES = [
    "HKQuantityTypeIdentifier",
    "HKCategoryTypeIdentifier",
    "HKDataType",
    "HKWorkoutActivityType",
]

# Map shortened type names to category folders.
# Prefix matches (e.g. "Dietary" matches "DietaryProtein", "DietaryFat", etc.)
CATEGORY_MAP = {
    # Activity
    "StepCount": "activity",
    "DistanceWalkingRunning": "activity",
    "DistanceCycling": "activity",
    "DistanceSwimming": "activity",
    "DistanceWheelchair": "activity",
    "FlightsClimbed": "activity",
    "ActiveEnergyBurned": "activity",
    "BasalEnergyBurned": "activity",
    "AppleExerciseTime": "activity",
    "WalkingSpeed": "activity",
    "WalkingStepLength": "activity",
    "RunningSpeed": "activity",
    "CyclingSpeed": "activity",
    "CyclingCadence": "activity",
    "CyclingPower": "activity",
    "CyclingFunctionalThresholdPower": "activity",
    "RunningStrideLength": "activity",
    "RunningVerticalOscillation": "activity",
    "RunningGroundContactTime": "activity",
    "RunningPower": "activity",
    "PhysicalEffort": "activity",
    "AppleMoveTime": "activity",
    "AppleStandTime": "activity",
    "NikeFuel": "activity",
    # Heart
    "HeartRate": "heart",
    "RestingHeartRate": "heart",
    "WalkingHeartRateAverage": "heart",
    "HeartRateVariabilitySDNN": "heart",
    "HeartRateRecoveryOneMinute": "heart",
    "VO2Max": "heart",
    "BloodPressureSystolic": "heart",
    "BloodPressureDiastolic": "heart",
    "AtrialFibrillationBurden": "heart",
    "PeripheralPerfusionIndex": "heart",
    # Body
    "BodyMass": "body",
    "BodyMassIndex": "body",
    "BodyFatPercentage": "body",
    "Height": "body",
    "LeanBodyMass": "body",
    "WaistCircumference": "body",
    "BodyTemperature": "body",
    "ElectrodermalActivity": "body",
    "AppleSleepingWristTemperature": "body",
    # Sleep
    "SleepAnalysis": "sleep",
    "SleepDurationGoal": "sleep",
    # Respiratory
    "OxygenSaturation": "respiratory",
    "RespiratoryRate": "respiratory",
    "ForcedExpiratoryVolume1": "respiratory",
    "ForcedVitalCapacity": "respiratory",
    "PeakExpiratoryFlowRate": "respiratory",
    "InhalerUsage": "respiratory",
    "SixMinuteWalkTestDistance": "respiratory",
    # Nutrition (prefix match below handles Dietary*)
    "WaterConsumption": "nutrition",
    # Mindfulness
    "MindfulSession": "mindfulness",
    # Audio
    "EnvironmentalAudioExposure": "audio",
    "HeadphoneAudioExposure": "audio",
    "EnvironmentalSoundReduction": "audio",
    # Blood / Lab
    "BloodGlucose": "lab_results",
    "BloodAlcoholContent": "lab_results",
    "InsulinDelivery": "lab_results",
    "NumberOfTimesFallen": "lab_results",
    # Reproductive
    "BasalBodyTemperature": "reproductive",
    "CervicalMucusQuality": "reproductive",
    "MenstrualFlow": "reproductive",
    "OvulationTestResult": "reproductive",
    "SexualActivity": "reproductive",
    # Mobility
    "WalkingDoubleSupportPercentage": "mobility",
    "WalkingAsymmetryPercentage": "mobility",
    "StairAscentSpeed": "mobility",
    "StairDescentSpeed": "mobility",
    "AppleWalkingSteadiness": "mobility",
    # UV
    "UVExposure": "other",
}

# Prefix-based category matching (for types like DietaryProtein, DietaryFat, etc.)
CATEGORY_PREFIXES = [
    ("Dietary", "nutrition"),
    ("Distance", "activity"),
]

RECORD_FIELDS = ["startDate", "endDate", "value", "src"]
WORKOUT_FIELDS = [
    "startDate", "endDate", "activityType", "duration", "durationUnit",
    "totalDistance", "distanceUnit", "totalEnergyBurned", "energyUnit", "src",
]
ACTIVITY_FIELDS = [
    "date", "activeEnergyBurned", "activeEnergyBurnedGoal",
    "exerciseMinutes", "exerciseGoal", "standHours", "standHoursGoal",
]


def parse_date(date_str):
    """Parse Apple Health date format '2025-03-04 08:15:32 -0700' to datetime.

    Strips the timezone offset for simple comparison. Returns None on failure.
    """
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str[:19], "%Y-%m-%d %H:%M:%S")
    except (ValueError, IndexError):
        return None


def shorten_type(type_name):
    """Strip verbose HK prefixes from type identifiers."""
    for prefix in TYPE_PREFIXES:
        if type_name.startswith(prefix):
            return type_name[len(prefix):]
    return type_name


def get_category(short_type):
    """Return the category folder name for a given shortened type."""
    if short_type in CATEGORY_MAP:
        return CATEGORY_MAP[short_type]
    for prefix, category in CATEGORY_PREFIXES:
        if short_type.startswith(prefix):
            return category
    return "other"


def to_snake_case(name):
    """Convert CamelCase to snake_case for filenames."""
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    return s.lower()


def format_date(date_str):
    """Trim Apple Health date to compact format without timezone."""
    if not date_str:
        return ""
    return date_str[:19]


class SourceShortener:
    """Assigns short codes to source names on-the-fly."""

    def __init__(self):
        self._map = {}
        self._counter = 0

    def shorten(self, source_name):
        if not source_name:
            return ""
        if source_name not in self._map:
            self._counter += 1
            self._map[source_name] = f"s{self._counter}"
        return self._map[source_name]

    def get_lookup(self):
        """Return {short_code: full_name} for summary.json."""
        return {v: k for k, v in self._map.items()}


class CSVWriterManager:
    """Lazily opens CSV files organized by category folder and year."""

    def __init__(self, output_dir):
        self.output_dir = output_dir
        self._writers = {}
        self._files = {}
        self._file_paths = {}  # (type, year) -> relative path from output_dir

    def write_row(self, type_name, category, year, fields, row):
        """Write a row to the CSV for the given type/year combo."""
        key = (type_name, year)
        if key not in self._writers:
            cat_dir = os.path.join(self.output_dir, category)
            os.makedirs(cat_dir, exist_ok=True)
            filename = f"{to_snake_case(type_name)}_{year}.csv"
            path = os.path.join(cat_dir, filename)
            f = open(path, "w", newline="", encoding="utf-8")
            writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            self._writers[key] = writer
            self._files[key] = f
            self._file_paths[key] = f"{category}/{filename}"
        self._writers[key].writerow(row)

    def close_all(self):
        for f in self._files.values():
            f.close()

    def get_file_list(self):
        """Return {category: [relative_paths]} for summary.json."""
        by_category = defaultdict(list)
        for (type_name, year), rel_path in sorted(self._file_paths.items()):
            category = rel_path.split("/")[0]
            by_category[category].append(rel_path.split("/")[1])
        return dict(by_category)

    def get_all_paths(self):
        """Return all relative file paths."""
        return sorted(self._file_paths.values())


def parse_export(input_path, days, output_dir, type_filter, verbose):
    """Stream-parse the export.xml and write categorized, yearly CSV outputs."""
    os.makedirs(output_dir, exist_ok=True)

    cutoff = None
    if days > 0:
        cutoff = datetime.now() - timedelta(days=days)

    csv_mgr = CSVWriterManager(output_dir)
    source_short = SourceShortener()

    # Stats tracking
    counts = defaultdict(int)
    units = {}
    years_seen = set()
    types_per_category = defaultdict(set)
    date_min = None
    date_max = None
    total_processed = 0
    total_skipped = 0

    try:
        context = iterparse(input_path, events=("end",))

        for event, elem in context:
            tag = elem.tag

            if tag == "Record":
                total_processed += 1
                try:
                    raw_type = elem.get("type", "")
                    short_type = shorten_type(raw_type)

                    if type_filter and short_type not in type_filter:
                        elem.clear()
                        continue

                    start = parse_date(elem.get("startDate"))

                    if cutoff and start and start < cutoff:
                        elem.clear()
                        continue

                    year = start.year if start else 0
                    category = get_category(short_type)

                    counts[short_type] += 1
                    years_seen.add(year)
                    types_per_category[category].add(short_type)

                    unit = elem.get("unit", "")
                    if unit and short_type not in units:
                        units[short_type] = unit

                    if start:
                        if date_min is None or start < date_min:
                            date_min = start
                        if date_max is None or start > date_max:
                            date_max = start

                    src = source_short.shorten(elem.get("sourceName", ""))
                    row = {
                        "startDate": format_date(elem.get("startDate")),
                        "endDate": format_date(elem.get("endDate")),
                        "value": elem.get("value", ""),
                        "src": src,
                    }
                    csv_mgr.write_row(short_type, category, year, RECORD_FIELDS, row)

                except Exception:
                    total_skipped += 1

                elem.clear()

            elif tag == "Workout":
                total_processed += 1
                try:
                    start = parse_date(elem.get("startDate"))

                    if cutoff and start and start < cutoff:
                        elem.clear()
                        continue

                    year = start.year if start else 0
                    activity = shorten_type(elem.get("workoutActivityType", ""))
                    counts["Workout"] += 1
                    years_seen.add(year)
                    types_per_category["workouts"].add("Workout")

                    if start:
                        if date_min is None or start < date_min:
                            date_min = start
                        if date_max is None or start > date_max:
                            date_max = start

                    src = source_short.shorten(elem.get("sourceName", ""))
                    row = {
                        "startDate": format_date(elem.get("startDate")),
                        "endDate": format_date(elem.get("endDate")),
                        "activityType": activity,
                        "duration": elem.get("duration", ""),
                        "durationUnit": elem.get("durationUnit", ""),
                        "totalDistance": elem.get("totalDistance", ""),
                        "distanceUnit": elem.get("totalDistanceUnit", ""),
                        "totalEnergyBurned": elem.get("totalEnergyBurned", ""),
                        "energyUnit": elem.get("totalEnergyBurnedUnit", ""),
                        "src": src,
                    }
                    csv_mgr.write_row("Workout", "workouts", year, WORKOUT_FIELDS, row)

                except Exception:
                    total_skipped += 1

                elem.clear()

            elif tag == "ActivitySummary":
                total_processed += 1
                try:
                    date_str = elem.get("dateComponents", "")
                    year = 0
                    if date_str:
                        try:
                            d = datetime.strptime(date_str, "%Y-%m-%d")
                            year = d.year
                            if cutoff and d < cutoff:
                                elem.clear()
                                continue
                        except ValueError:
                            pass

                    counts["ActivitySummary"] += 1
                    years_seen.add(year)
                    types_per_category["vitals"].add("ActivitySummary")

                    row = {
                        "date": date_str,
                        "activeEnergyBurned": elem.get("activeEnergyBurned", ""),
                        "activeEnergyBurnedGoal": elem.get("activeEnergyBurnedGoal", ""),
                        "exerciseMinutes": elem.get("appleExerciseTime", ""),
                        "exerciseGoal": elem.get("appleExerciseTimeGoal", ""),
                        "standHours": elem.get("appleStandHours", ""),
                        "standHoursGoal": elem.get("appleStandHoursGoal", ""),
                    }
                    csv_mgr.write_row(
                        "ActivitySummary", "vitals", year, ACTIVITY_FIELDS, row,
                    )

                except Exception:
                    total_skipped += 1

                elem.clear()

            else:
                elem.clear()

            if verbose and total_processed % 1_000_000 == 0:
                print(f"  Processed {total_processed:,} elements...", file=sys.stderr)

    except Exception as e:
        print(f"Warning: XML parsing stopped early: {e}", file=sys.stderr)
        print("Writing partial results...", file=sys.stderr)

    csv_mgr.close_all()

    # Build summary
    years_sorted = sorted(y for y in years_seen if y > 0)
    categories = {}
    for cat, types in sorted(types_per_category.items()):
        cat_files = csv_mgr.get_file_list().get(cat, [])
        categories[cat] = {
            "types": sorted(types),
            "files": sorted(cat_files),
        }

    summary = {
        "description": "Apple Health export parsed into categorized, yearly CSV files. "
                       "Source names in CSVs use short codes — see sourceLookup below.",
        "dateRange": {
            "start": date_min.strftime("%Y-%m-%d") if date_min else None,
            "end": date_max.strftime("%Y-%m-%d") if date_max else None,
        },
        "years": years_sorted,
        "categories": categories,
        "recordCounts": dict(sorted(counts.items(), key=lambda x: -x[1])),
        "units": units,
        "sourceLookup": source_short.get_lookup(),
        "totalRecords": sum(counts.values()),
        "totalSkipped": total_skipped,
    }

    summary_path = os.path.join(output_dir, "summary.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    return summary


def main():
    parser = argparse.ArgumentParser(
        description="Parse Apple Health export.xml into categorized, yearly CSV files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  %(prog)s export.xml                          Parse all history
  %(prog)s export.xml --days 90                Last 90 days only
  %(prog)s export.xml --types StepCount,HeartRate
  %(prog)s export.xml --verbose                Show progress""",
    )
    parser.add_argument("input", help="Path to Apple Health export.xml file")
    parser.add_argument(
        "--days", type=int, default=0,
        help="Days of history to include (default: 0 = all)",
    )
    parser.add_argument(
        "--output-dir", default="./output",
        help="Output directory (default: ./output)",
    )
    parser.add_argument(
        "--types",
        help="Comma-separated list of types to include (e.g., StepCount,HeartRate)",
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="Show progress every 1M records",
    )

    args = parser.parse_args()

    if not os.path.isfile(args.input):
        print(f"Error: File not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    type_filter = None
    if args.types:
        type_filter = set(args.types.split(","))

    print(f"Parsing {args.input}...")
    if args.days > 0:
        print(f"  Keeping last {args.days} days")
    else:
        print(f"  Keeping all history")
    if type_filter:
        print(f"  Filtering to types: {', '.join(sorted(type_filter))}")
    print(f"  Output: {args.output_dir}/")
    print()

    summary = parse_export(
        args.input, args.days, args.output_dir, type_filter, args.verbose,
    )

    print("Done!")
    print(f"  Date range: {summary['dateRange']['start']} to {summary['dateRange']['end']}")
    print(f"  Years: {', '.join(str(y) for y in summary['years'])}")
    print(f"  Records: {summary['totalRecords']:,}")
    if summary["totalSkipped"]:
        print(f"  Skipped: {summary['totalSkipped']:,}")
    print()
    print(f"  Categories:")
    for cat, info in sorted(summary["categories"].items()):
        total = sum(summary["recordCounts"].get(t, 0) for t in info["types"])
        print(f"    {cat}/ ({total:,} records, {len(info['files'])} files)")
    print()
    print(f"  Sources:")
    for code, name in sorted(summary["sourceLookup"].items()):
        print(f"    {code} = {name}")
    print()
    print(f"  Output: {args.output_dir}/summary.json + {len(summary['categories'])} category folders")


if __name__ == "__main__":
    main()
