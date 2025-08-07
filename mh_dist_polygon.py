import requests
import json
from shapely.wkt import loads
from shapely.geometry import mapping
import geopandas as gpd
from pyproj import Transformer
import time
import os
import random
from shapely.ops import transform

class BhuNakshaExtractor:
    def __init__(self, state="27", category='U'):
        self.base_url = "https://mahabhunakasha.mahabhumi.gov.in"
        self.state = state
        self.category = category
        self.session = requests.Session()
        self._initialize_session()
        self.transformer = Transformer.from_crs("EPSG:32643", "EPSG:4326", always_xy=True)

    def _initialize_session(self):
        """Initializes the session and sets headers."""
        print("Initializing session...")
        main_page_url = f"{self.base_url}/{self.state}/index.html"
        try:
            self.session.get(main_page_url, timeout=10)
        except requests.exceptions.RequestException as e:
            print(f"Warning: Could not initialize session: {e}")
        
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:142.0) Gecko/20100101 Firefox/142.0',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'X-Requested-With': 'XMLHttpRequest',
            'Origin': 'https://mahabhunakasha.mahabhumi.gov.in',
            'Connection': 'keep-alive',
            'Referer': f'https://mahabhunakasha.mahabhumi.gov.in/{self.state}/index.html',
        })

    def get_hierarchy_data(self, level, codes_str):
        """Generic function to get dropdown lists (districts, taluks, villages)."""
        url = f"{self.base_url}/rest/VillageMapService/ListsAfterLevelGeoref"
        payload = {"state": self.state, "level": level, "codes": codes_str, "hasmap": "true"}
        try:
            response = self.session.post(url, data=payload, timeout=15)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Error getting hierarchy data for level {level}, codes {codes_str}: {e}")
            return None

    def get_village_info(self, gis_code):
        """Gets metadata for an entire village, like extent and attribution."""
        url = f"{self.base_url}/rest/MapInfo/getVVVVExtentGeoref"
        payload = {"state": self.state, "giscode": gis_code, "srs": "4326"}
        try:
            response = self.session.post(url, data=payload, timeout=15)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Error getting village info for {gis_code}: {e}")
            return None

    def get_village_gis_code(self, map_type_code, district_code, taluk_code, village_code):
        """Constructs the final GIS code from its constituent parts."""
        if not all([self.category, map_type_code, district_code, taluk_code, village_code]):
            return None
        return f"{self.category}{map_type_code}{district_code}{taluk_code}{village_code}"

    def get_plot_list(self, gis_code):
        """Gets the list of all plot numbers in a given village/area."""
        url = f"{self.base_url}/rest/VillageMapService/kidelistFromGisCodeMH"
        payload = {"state": self.state, "logedLevels": gis_code}
        try:
            response = self.session.post(url, data=payload, timeout=15)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Error getting plot list for {gis_code}: {e}")
            return []
    
    def get_plot_geometry(self, gis_code, plot_no):
        """Gets the geometry and info for a single plot."""
        url = f"{self.base_url}/rest/MapInfo/getPlotInfo"
        payload = {"state": self.state, "giscode": gis_code, "plotno": plot_no, "srs": "4326"}
        try:
            response = self.session.post(url, data=payload, timeout=15)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Error getting plot {plot_no} geometry: {e}")
            return None
    
    def wkt_to_geojson(self, wkt_string):
        """Converts WKT to GeoJSON and reprojects coordinates."""
        try:
            geom = loads(wkt_string)
            reprojected_geom = transform(self.transformer.transform, geom)
            return mapping(reprojected_geom)
        except Exception as e:
            print(f"Error converting WKT to GeoJSON: {e}")
            return None
    
    def extract_and_save_village_data(self, gis_code, district_name, taluk_name, village_name, output_dir, delay_range=(1, 3)):
        """Extracts and saves plot data, including all details, with resume capability."""
        taluk_dir = os.path.join(output_dir, taluk_name.replace(' ', '_'))
        os.makedirs(taluk_dir, exist_ok=True)
        output_path = os.path.join(taluk_dir, f"{village_name.replace(' ', '_').replace('/', '_')}.geojson")

        all_features = []
        existing_plot_nos = set()
        village_info = {}

        if os.path.exists(output_path):
            try:
                with open(output_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    all_features = data.get('features', [])
                    village_info = data.get('metadata', {}).get('village_info', {})
                    for feature in all_features:
                        # Check for plot_no in the nested API data
                        if 'plotno' in feature.get('properties', {}):
                            existing_plot_nos.add(feature['properties']['plotno'])
                print(f"    -> Found existing file for '{village_name}' with {len(existing_plot_nos)} plots. Resuming...")
            except (json.JSONDecodeError, IOError) as e:
                print(f"    -> Warning: Could not read existing file for '{village_name}' due to error: {e}. Starting fresh.")
                all_features = []
                existing_plot_nos = set()

        if not gis_code:
            print(f"    ❌ Skipping extraction for '{village_name}' due to invalid gis_code.")
            return

        if not village_info:
            village_info = self.get_village_info(gis_code)
            
        plot_list = self.get_plot_list(gis_code)
        if not plot_list:
            print(f"      -> Could not fetch plot list for {gis_code}. Skipping.")
            return
        
        total_plots = len(plot_list)
        print(f"    ✅ Village '{village_name}' has {total_plots} total plots.")

        if len(existing_plot_nos) == total_plots:
            print(f"    -> All {total_plots} plots for '{village_name}' already scraped. Skipping.")
            return

        for i, plot_no in enumerate(plot_list):
            if plot_no in existing_plot_nos:
                continue

            print(f"      -> Processing plot {plot_no} ({i+1}/{total_plots})")
            plot_data = self.get_plot_geometry(gis_code, plot_no)
            
            if plot_data and plot_data.get('the_geom'):
                geometry = self.wkt_to_geojson(plot_data['the_geom'])
                if geometry:
                    # Create properties dictionary with context
                    properties = {
                        "district": district_name,
                        "taluk": taluk_name,
                        "village": village_name,
                    }
                    # Add the entire raw API response to the properties
                    properties.update(plot_data)
                    
                    all_features.append({
                        "type": "Feature",
                        "geometry": geometry,
                        "properties": properties
                    })
                    
                    geojson_output = {
                        "type": "FeatureCollection",
                        "metadata": {
                            "district": district_name,
                            "taluk": taluk_name,
                            "village": village_name,
                            "village_info": village_info,
                            "total_plots": total_plots,
                            "successful_plots": len(all_features),
                            "failed_plots": (i + 1) - len(all_features),
                            "gis_code": gis_code,
                            "state": self.state
                        },
                        "features": all_features
                    }
                    try:
                        with open(output_path, 'w', encoding='utf-8') as f:
                            json.dump(geojson_output, f, ensure_ascii=False, indent=2)
                        existing_plot_nos.add(plot_no)
                    except Exception as e:
                        print(f"      -> ❌ Error saving incremental progress to GeoJSON file: {e}")
            
            sleep_time = random.uniform(delay_range[0], delay_range[1])
            time.sleep(sleep_time)

        print(f"      -> 💾 Finished processing for '{village_name}'. Total plots saved: {len(all_features)}.")


    def scrape_districts(self, target_districts, delay_per_plot_range=(1, 3)):
        """Main function to iterate through districts, taluks, and villages."""
        print(f"Starting scrape for districts: {target_districts}")
        district_response = self.get_hierarchy_data(level=1, codes_str=f"{self.category},")
        if not district_response or not district_response[0]:
            print("❌ Could not fetch district list. Aborting.")
            return
        all_districts = {item['value']: item['code'] for item in district_response[0]}
        
        for district_name in target_districts:
            district_code = all_districts.get(district_name)
            if not district_code:
                print(f"District '{district_name}' not found. Skipping.")
                continue
            
            print(f"\nProcessing District: {district_name} (Code: {district_code})")
            district_dir = f"data_{district_name.replace(' ', '_')}"
            os.makedirs(district_dir, exist_ok=True)
            
            print("  -> Discovering all taluks...")
            taluk_response = self.get_hierarchy_data(level=2, codes_str=f"{self.category},{district_code},")
            if not taluk_response or not taluk_response[0]:
                print(f"    -> Could not fetch taluks for {district_name}. Skipping district.")
                continue
            taluks_to_process = {item['value']: item['code'] for item in taluk_response[0]}
            print(f"  -> Found {len(taluks_to_process)} taluks.")

            for taluk_name, taluk_code in taluks_to_process.items():
                print(f"  -> Processing Taluk: {taluk_name} (Code: {taluk_code})")
                
                village_response = self.get_hierarchy_data(level=3, codes_str=f"{self.category},{district_code},{taluk_code},")
                
                if not village_response or not village_response[0]:
                    print(f"    -> No villages/areas found within {taluk_name}. Skipping taluk.")
                    continue

                map_type_code = village_response[1][0]['code'] if village_response[1] else None
                all_villages = {item['value']: item['code'] for item in village_response[0]}
                
                for village_name, village_code in all_villages.items():
                    gis_code = self.get_village_gis_code(map_type_code, district_code, taluk_code, village_code)
                    self.extract_and_save_village_data(gis_code, district_name, taluk_name, village_name, district_dir, delay_per_plot_range)


if __name__ == "__main__":
    CATEGORY = 'U' 
    TARGET_DISTRICTS = ["Mumbai City", "Pune", "Mumbai Sub-Urban"] 
    DELAY_BETWEEN_PLOTS_RANGE = (1, 3)
    
    extractor = BhuNakshaExtractor(state="27", category=CATEGORY)
    extractor.scrape_districts(
        target_districts=TARGET_DISTRICTS,
        delay_per_plot_range=DELAY_BETWEEN_PLOTS_RANGE
    )
    print("\nScraping process finished.")
