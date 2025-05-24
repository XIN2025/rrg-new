#!/usr/bin/env python3

import json
import sys
from pathlib import Path
from typing import Dict, List, Any, Tuple, Set
from datetime import datetime
import re

class JsonComparator:
    def __init__(self, prod_file_path: str, local_file_path: str):
        """Initialize the comparator with paths to the production and local JSON files.
        
        Args:
            prod_file_path: Path to the production JSON file (source of truth)
            local_file_path: Path to the local JSON file
        """
        self.prod_file_path = prod_file_path
        self.local_file_path = local_file_path
        self.prod_data = self._load_json(prod_file_path)
        self.local_data = self._load_json(local_file_path)
        
        self.prod_name = Path(prod_file_path).name
        self.local_name = Path(local_file_path).name
        
    def _load_json(self, file_path: str) -> Dict:
        """Load a JSON file into a Python dictionary.
        
        Args:
            file_path: Path to the JSON file
            
        Returns:
            The parsed JSON data as a dictionary
        """
        try:
            with open(file_path, 'r') as f:
                content = f.read()
                # Fix any trailing commas if they exist (common JSON issue)
                content = re.sub(r',\s*}', '}', content)
                content = re.sub(r',\s*]', ']', content)
                return json.loads(content)
        except json.JSONDecodeError as e:
            print(f"Error: {file_path} contains invalid JSON: {str(e)}")
            # For debugging, show the problematic area
            error_line = e.lineno
            with open(file_path, 'r') as f:
                lines = f.readlines()
                start_line = max(0, error_line - 5)
                end_line = min(len(lines), error_line + 5)
                print(f"Error context (lines {start_line+1}-{end_line}):")
                for i in range(start_line, end_line):
                    prefix = "-> " if i == error_line - 1 else "   "
                    print(f"{prefix}{i+1}: {lines[i].rstrip()}")
            sys.exit(1)
        except FileNotFoundError:
            print(f"Error: File {file_path} not found")
            sys.exit(1)
    
    def _normalize_value(self, value: Any) -> Any:
        """Normalize values for comparison to avoid trivial formatting differences.
        
        Args:
            value: The value to normalize
            
        Returns:
            The normalized value
        """
        if isinstance(value, str):
            # If it's a date string with or without time
            if re.match(r'^\d{4}-\d{2}-\d{2}( \d{2}:\d{2}:\d{2})?$', value):
                # Strip the time portion if it exists
                return value.split(' ')[0]
            return value
        elif isinstance(value, (int, float)):
            # Convert to string and standardize to avoid precision differences
            return f"{float(value):.2f}"
        return value
    
    def compare_benchmarks(self) -> Tuple[bool, str]:
        """Compare the benchmark values between prod and local.
        
        Returns:
            A tuple containing a boolean indicating if they match and a message
        """
        prod_benchmark = self.prod_data.get('data', {}).get('benchmark')
        local_benchmark = self.local_data.get('data', {}).get('benchmark')
        
        if prod_benchmark == local_benchmark:
            return True, f"Benchmarks match: {prod_benchmark}"
        else:
            return False, f"Benchmarks differ: prod={prod_benchmark}, local={local_benchmark}"
    
    def compare_indexdata(self) -> Tuple[bool, List[str], List[int], List[int]]:
        """Compare the indexdata arrays between prod and local.
        
        Returns:
            A tuple containing:
            - boolean indicating if they match
            - list of differences
            - list of indices missing in local
            - list of extra indices in local
        """
        prod_indexdata = self.prod_data.get('data', {}).get('indexdata', [])
        local_indexdata = self.local_data.get('data', {}).get('indexdata', [])
        
        if not prod_indexdata or not local_indexdata:
            return False, ["One or both files are missing indexdata"], [], []
        
        differences = []
        missing_in_local = []
        extra_in_local = []
        
        # Compare lengths
        if len(prod_indexdata) != len(local_indexdata):
            differences.append(f"Indexdata length differs: prod={len(prod_indexdata)}, local={len(local_indexdata)}")
            
            # If prod has more data, some is missing in local
            if len(prod_indexdata) > len(local_indexdata):
                missing_in_local = list(range(len(local_indexdata), len(prod_indexdata)))
            # If local has more data, it has extra entries
            else:
                extra_in_local = list(range(len(prod_indexdata), len(local_indexdata)))
        
        # Compare common indices
        common_length = min(len(prod_indexdata), len(local_indexdata))
        for i in range(common_length):
            # Normalize for comparison to avoid trivial differences
            prod_value = self._normalize_value(prod_indexdata[i])
            local_value = self._normalize_value(local_indexdata[i])
            
            if prod_value != local_value:
                differences.append(f"Indexdata differs at position {i}: prod={prod_indexdata[i]}, local={local_indexdata[i]}")
        
        return len(differences) == 0, differences, missing_in_local, extra_in_local
    
    def compare_datalists(self) -> Tuple[bool, List[str], List[str], List[str]]:
        """Compare the datalists between prod and local.
        
        Returns:
            A tuple containing:
            - boolean indicating if they match
            - list of differences
            - list of symbols missing in local
            - list of extra symbols in local
        """
        prod_datalists = self.prod_data.get('data', {}).get('datalists', [])
        local_datalists = self.local_data.get('data', {}).get('datalists', [])
        
        if not prod_datalists or not local_datalists:
            return False, ["One or both files are missing datalists"], [], []
        
        differences = []
        
        # Get symbols from both datalists for comparison
        prod_symbols = {item.get('symbol'): item for item in prod_datalists if 'symbol' in item}
        local_symbols = {item.get('symbol'): item for item in local_datalists if 'symbol' in item}
        
        # Filter out None keys if they exist
        if None in prod_symbols:
            del prod_symbols[None]
        if None in local_symbols:
            del local_symbols[None]
        
        # Check for symbols in prod but not in local
        missing_in_local = list(set(prod_symbols.keys()) - set(local_symbols.keys()))
        if missing_in_local:
            # Filter out any None values that might have slipped through
            missing_in_local = [symbol for symbol in missing_in_local if symbol is not None]
            if missing_in_local:
                differences.append(f"Symbols missing in local: {', '.join(sorted(missing_in_local))}")
        
        # Check for symbols in local but not in prod
        extra_in_local = list(set(local_symbols.keys()) - set(prod_symbols.keys()))
        if extra_in_local:
            # Filter out any None values
            extra_in_local = [symbol for symbol in extra_in_local if symbol is not None]
            if extra_in_local:
                differences.append(f"Extra symbols in local: {', '.join(sorted(extra_in_local))}")
        
        # Compare data for common symbols (focusing on structure, not exact values)
        common_symbols = set(prod_symbols.keys()) & set(local_symbols.keys())
        for symbol in common_symbols:
            prod_item = prod_symbols[symbol]
            local_item = local_symbols[symbol]
            
            # Check if data arrays exist
            if 'data' not in prod_item:
                differences.append(f"Symbol {symbol} is missing 'data' array in prod")
                continue
                
            if 'data' not in local_item:
                differences.append(f"Symbol {symbol} is missing 'data' array in local")
                continue
            
            # Compare data array lengths
            prod_data = prod_item['data']
            local_data = local_item['data']
            
            if len(prod_data) != len(local_data):
                differences.append(f"Data length differs for symbol {symbol}: prod={len(prod_data)}, local={len(local_data)}")
            
            # Compare data structure (not exact values)
            if len(prod_data) > 0 and len(local_data) > 0:
                # Check if the structure is the same by comparing the first row
                if len(prod_data[0]) != len(local_data[0]):
                    differences.append(f"Data structure differs for symbol {symbol}: prod has {len(prod_data[0])} columns, local has {len(local_data[0])} columns")
        
        return len(differences) == 0, differences, missing_in_local, extra_in_local
    
    def compare_change_data(self) -> Tuple[bool, List[str], List[str], List[str]]:
        """Compare the change_data between prod and local.
        
        Returns:
            A tuple containing:
            - boolean indicating if they match
            - list of differences
            - list of symbols missing in local
            - list of extra symbols in local
        """
        prod_change_data = self.prod_data.get('change_data', [])
        local_change_data = self.local_data.get('change_data', [])
        
        if prod_change_data is None and local_change_data is None:
            return True, ["Both files don't have change_data"], [], []
        
        if prod_change_data is None:
            return False, ["prod does not have change_data but local does"], [], []
            
        if local_change_data is None:
            return False, ["local does not have change_data but prod does"], [], []
        
        differences = []
        
        # Create dictionaries for easier comparison
        prod_change_data_dict = {item.get('symbol'): item for item in prod_change_data if 'symbol' in item}
        local_change_data_dict = {item.get('symbol'): item for item in local_change_data if 'symbol' in item}
        
        # Filter out None keys if they exist
        if None in prod_change_data_dict:
            del prod_change_data_dict[None]
        if None in local_change_data_dict:
            del local_change_data_dict[None]
        
        # Check for symbols in prod but not in local
        missing_in_local = list(set(prod_change_data_dict.keys()) - set(local_change_data_dict.keys()))
        if missing_in_local:
            # Filter out any None values
            missing_in_local = [symbol for symbol in missing_in_local if symbol is not None]
            if missing_in_local:
                differences.append(f"Change data symbols missing in local: {', '.join(sorted(missing_in_local))}")
        
        # Check for symbols in local but not in prod
        extra_in_local = list(set(local_change_data_dict.keys()) - set(prod_change_data_dict.keys()))
        if extra_in_local:
            # Filter out any None values
            extra_in_local = [symbol for symbol in extra_in_local if symbol is not None]
            if extra_in_local:
                differences.append(f"Extra change data symbols in local: {', '.join(sorted(extra_in_local))}")
        
        # Compare common symbols structure (ignoring value precision differences)
        common_symbols = set(prod_change_data_dict.keys()) & set(local_change_data_dict.keys())
        for symbol in common_symbols:
            prod_item = prod_change_data_dict[symbol]
            local_item = local_change_data_dict[symbol]
            
            # Check if they have the same keys
            prod_keys = set(prod_item.keys())
            local_keys = set(local_item.keys())
            
            # Keys missing in local
            missing_keys = prod_keys - local_keys
            if missing_keys:
                differences.append(f"Symbol {symbol} is missing keys in local: {', '.join(missing_keys)}")
            
            # Extra keys in local
            extra_keys = local_keys - prod_keys
            if extra_keys:
                differences.append(f"Symbol {symbol} has extra keys in local: {', '.join(extra_keys)}")
        
        return len(differences) == 0, differences, missing_in_local, extra_in_local
    
    def compare_other_fields(self) -> List[str]:
        """Compare other top-level fields between prod and local.
        
        Returns:
            A list of differences
        """
        differences = []
        
        # Get all top-level keys excluding the ones we've already compared
        prod_keys = set(self.prod_data.keys()) - {'data', 'change_data'}
        local_keys = set(self.local_data.keys()) - {'data', 'change_data'}
        
        # Check for keys in prod but not in local
        missing_in_local = prod_keys - local_keys
        if missing_in_local:
            differences.append(f"Fields missing in local: {', '.join(sorted(missing_in_local))}")
        
        # Check for keys in local but not in prod
        extra_in_local = local_keys - prod_keys
        if extra_in_local:
            differences.append(f"Extra fields in local: {', '.join(sorted(extra_in_local))}")
        
        return differences
    
    def compare_all(self) -> Dict[str, Any]:
        """Run all comparisons and return a summary of the results.
        
        Returns:
            A dictionary containing the results of all comparisons
        """
        benchmark_match, benchmark_message = self.compare_benchmarks()
        indexdata_match, indexdata_differences, indexdata_missing, indexdata_extra = self.compare_indexdata()
        datalists_match, datalists_differences, datalists_missing, datalists_extra = self.compare_datalists()
        change_data_match, change_data_differences, change_data_missing, change_data_extra = self.compare_change_data()
        other_differences = self.compare_other_fields()
        
        summary = {
            "timestamp": datetime.now().isoformat(),
            "prod_file": self.prod_file_path,
            "local_file": self.local_file_path,
            "overall_match": benchmark_match and indexdata_match and datalists_match and change_data_match and not other_differences,
            "sections": {
                "benchmark": {
                    "match": benchmark_match,
                    "message": benchmark_message
                },
                "indexdata": {
                    "match": indexdata_match,
                    "differences": indexdata_differences,
                    "missing_in_local": indexdata_missing,
                    "extra_in_local": indexdata_extra
                },
                "datalists": {
                    "match": datalists_match,
                    "differences": datalists_differences,
                    "missing_in_local": datalists_missing,
                    "extra_in_local": datalists_extra,
                    "total_differences": len(datalists_differences)
                },
                "change_data": {
                    "match": change_data_match,
                    "differences": change_data_differences,
                    "missing_in_local": change_data_missing,
                    "extra_in_local": change_data_extra
                },
                "other_fields": {
                    "match": not other_differences,
                    "differences": other_differences
                }
            }
        }
        
        return summary

def find_prod_and_local_files(directory: Path) -> Tuple[str, str]:
    """Find the production and local files in a directory.
    
    Args:
        directory: Path to the directory containing the files
        
    Returns:
        A tuple containing the paths to the production and local files
    """
    files = list(directory.glob("*.json"))
    
    if len(files) != 2:
        print(f"Expected 2 JSON files in {directory}, found {len(files)}")
        sys.exit(1)
    
    prod_file = None
    local_file = None
    
    for file in files:
        if file.name.startswith("prod_"):
            prod_file = str(file)
        elif file.name.startswith("local_"):
            local_file = str(file)
    
    if not prod_file or not local_file:
        print(f"Could not identify prod and local files in {directory}")
        print(f"Available files: {[f.name for f in files]}")
        sys.exit(1)
    
    return prod_file, local_file

def main():
    """Main function to run the comparison and print the results."""
    debug_dir = Path("debug_responses")
    
    prod_file_path, local_file_path = find_prod_and_local_files(debug_dir)
    
    comparator = JsonComparator(prod_file_path, local_file_path)
    result = comparator.compare_all()
    
    # Print a summary
    print(f"Comparison of prod ({Path(prod_file_path).name}) and local ({Path(local_file_path).name}):")
    print(f"Timestamp: {result['timestamp']}")
    print(f"Overall match: {'Yes' if result['overall_match'] else 'No'}")
    print("\nSection Results:")
    
    for section, details in result['sections'].items():
        print(f"  {section.capitalize()}: {'Match' if details['match'] else 'Mismatch'}")
    
    # Save detailed results to files
    with open("comparison_results.json", "w") as f:
        json.dump(result, f, indent=2)
    
    # Create a summary file specifically for missing/extra items
    with open("missing_and_extra.txt", "w") as f:
        f.write(f"COMPARISON SUMMARY: MISSING AND EXTRA ITEMS\n")
        f.write(f"Prod file: {prod_file_path}\n")
        f.write(f"Local file: {local_file_path}\n")
        f.write(f"Generated: {result['timestamp']}\n\n")
        
        # Indexdata
        f.write("=== INDEXDATA ===\n")
        if result['sections']['indexdata']['missing_in_local']:
            f.write("Missing in local:\n")
            for idx in result['sections']['indexdata']['missing_in_local']:
                f.write(f"  - Index position {idx}\n")
        else:
            f.write("No missing indexdata in local\n")
            
        if result['sections']['indexdata']['extra_in_local']:
            f.write("\nExtra in local:\n")
            for idx in result['sections']['indexdata']['extra_in_local']:
                f.write(f"  - Index position {idx}\n")
        else:
            f.write("No extra indexdata in local\n")
        
        # Datalists
        f.write("\n=== DATALISTS (SYMBOLS) ===\n")
        if result['sections']['datalists']['missing_in_local']:
            f.write("Missing symbols in local:\n")
            for symbol in sorted(result['sections']['datalists']['missing_in_local']):
                f.write(f"  - {symbol}\n")
        else:
            f.write("No missing symbols in local\n")
            
        if result['sections']['datalists']['extra_in_local']:
            f.write("\nExtra symbols in local:\n")
            for symbol in sorted(result['sections']['datalists']['extra_in_local']):
                f.write(f"  - {symbol}\n")
        else:
            f.write("No extra symbols in local\n")
        
        # Change Data
        f.write("\n=== CHANGE DATA ===\n")
        if result['sections']['change_data']['missing_in_local']:
            f.write("Missing change data symbols in local:\n")
            for symbol in sorted(result['sections']['change_data']['missing_in_local']):
                f.write(f"  - {symbol}\n")
        else:
            f.write("No missing change data symbols in local\n")
            
        if result['sections']['change_data']['extra_in_local']:
            f.write("\nExtra change data symbols in local:\n")
            for symbol in sorted(result['sections']['change_data']['extra_in_local']):
                f.write(f"  - {symbol}\n")
        else:
            f.write("No extra change data symbols in local\n")
        
        # Other Fields
        f.write("\n=== OTHER FIELDS ===\n")
        if result['sections']['other_fields']['differences']:
            for diff in result['sections']['other_fields']['differences']:
                f.write(f"  {diff}\n")
        else:
            f.write("No differences in other fields\n")
    
    print(f"\nDetailed comparison results saved to comparison_results.json")
    print(f"Summary of missing and extra items saved to missing_and_extra.txt")

if __name__ == "__main__":
    main() 