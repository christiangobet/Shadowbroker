// ─── ShadowBroker Dashboard Data Types ─────────────────────────────────────
// Canonical type definitions for all data flowing from backend → frontend.
// Every `any` in the codebase should eventually be replaced with these types.

// ─── FLIGHTS ────────────────────────────────────────────────────────────────

export interface FlightBase {
  callsign: string;
  country: string;
  lat: number;
  lng: number;
  alt: number;
  heading: number;
  speed_knots: number | null;
  registration: string;
  model: string;
  icao24: string;
  squawk?: string;
  aircraft_category?: string;
  nac_p?: number;
  _seen_at?: number;
  origin_loc?: [number, number] | null;
  dest_loc?: [number, number] | null;
  origin_name?: string;
  dest_name?: string;
  trail?: Array<{ lat: number; lng: number; alt?: number; ts?: number }>;
  holding?: boolean;
}

export interface CommercialFlight extends FlightBase {
  type: "commercial_flight";
  airline_code?: string;
  supplemental_source?: string;
}

export interface PrivateFlight extends FlightBase {
  type: "private_ga" | "private_flight";
}

export interface PrivateJet extends FlightBase {
  type: "private_jet";
}

export interface MilitaryFlight extends FlightBase {
  type: "military_flight";
  military_type?: "heli" | "fighter" | "bomber" | "tanker" | "cargo" | "recon" | "default";
  force?: string;
}

export interface TrackedFlight extends FlightBase {
  type: "tracked_flight";
  alert_category?: string;
  alert_operator?: string;
  alert_special?: string;
  alert_flag?: string;
  alert_color?: string;
  alert_wiki?: string;
  alert_type?: string;
  alert_tags?: string[];
  alert_link?: string;
  tracked_name?: string;
  operator?: string;
  owner?: string;
  name?: string;
}

export interface UAV extends FlightBase {
  type: "uav";
  uav_type?: string;
  aircraft_model?: string;
  wiki?: string;
  force?: string;
}

export type Flight = CommercialFlight | PrivateFlight | PrivateJet | MilitaryFlight | TrackedFlight | UAV;

// ─── SHIPS / MARITIME ───────────────────────────────────────────────────────

export interface Ship {
  mmsi: number;
  name: string;
  type: "carrier" | "military_vessel" | "tanker" | "cargo" | "passenger" | "yacht" | "other" | "unknown";
  lat: number;
  lng: number;
  heading: number;
  sog: number;
  cog: number;
  callsign?: string;
  destination?: string;
  imo?: number;
  country: string;
  ais_type_code?: number;
  _updated?: number;
  estimated?: boolean;
  source?: string;
  source_url?: string;
  last_osint_update?: string;
  desc?: string;
  // Tracked yacht enrichment
  yacht_alert?: boolean;
  yacht_owner?: string;
  yacht_name?: string;
  yacht_category?: string;
  yacht_color?: string;
  yacht_builder?: string;
  yacht_length?: number;
  yacht_year?: number;
  yacht_link?: string;
  // PLAN/CCG vessel enrichment
  plan_name?: string;
  plan_class?: string;
  plan_force?: string;
  plan_hull?: string;
  plan_wiki?: string;
  // Carrier enrichment
  wiki?: string;
  homeport?: string;
  homeport_lat?: number;
  homeport_lng?: number;
  fallback_lat?: number;
  fallback_lng?: number;
  fallback_heading?: number;
  fallback_desc?: string;
}

// ─── SATELLITES ─────────────────────────────────────────────────────────────

export type SatelliteMission =
  | "military_recon" | "military_sar" | "military_ew"
  | "sar" | "commercial_imaging" | "navigation"
  | "early_warning" | "space_station" | "sigint" | "general";

export interface Satellite {
  id: number;
  name: string;
  mission: SatelliteMission;
  sat_type: string;
  country: string;
  wiki?: string;
  lat: number;
  lng: number;
  alt_km: number;
  speed_knots: number;
  heading: number;
}

// ─── EARTHQUAKES ────────────────────────────────────────────────────────────

export interface Earthquake {
  id: string;
  mag: number;
  lat: number;
  lng: number;
  place: string;
  title?: string;
}

// ─── GPS JAMMING ────────────────────────────────────────────────────────────

export interface GPSJammingZone {
  lat: number;
  lng: number;
  severity: "high" | "medium" | "low";
  ratio: number;
  degraded: number;
  total: number;
}

// ─── FIRE HOTSPOTS (NASA FIRMS) ─────────────────────────────────────────────

export interface FireHotspot {
  lat: number;
  lng: number;
  frp: number;
  brightness: number;
  confidence: string;
  daynight: string;
  acq_date: string;
  acq_time: string;
}

// ─── CCTV CAMERAS ───────────────────────────────────────────────────────────

export interface CCTVCamera {
  id: string | number;
  lat: number;
  lon: number;
  direction_facing?: string;
  source_agency?: string;
  media_url?: string;
  media_type?: "image" | "hls" | "mjpeg";
}

// ─── KIWISDR RECEIVERS ─────────────────────────────────────────────────────

export interface KiwiSDR {
  lat: number;
  lon: number;
  name: string;
  url?: string;
  users?: number;
  users_max?: number;
  bands?: string;
  antenna?: string;
  location?: string;
}

// ─── INTERNET OUTAGES (IODA) ────────────────────────────────────────────────

export interface InternetOutage {
  region_code: string;
  region_name: string;
  country_code: string;
  country_name: string;
  level: string;
  datasource: string;
  severity: number;
  lat: number;
  lng: number;
}

// ─── DATA CENTERS ───────────────────────────────────────────────────────────

export interface DataCenter {
  // ── Identity ──────────────────────────────────────────────────────────────
  name: string;
  company: string;
  street?: string;
  city?: string;
  country?: string;
  zip?: string;
  lat: number;
  lng: number;

  // ── Layer 1: Physical asset ───────────────────────────────────────────────
  // Populated by: scripts/enrich_l1_physical.py (OSM + bgeesaman dataset)
  operator_type?: "hyperscaler" | "colocation" | "enterprise" | null;
  tier_rating?: 1 | 2 | 3 | 4 | null;        // Uptime Institute Tier I–IV
  mw_capacity?: number | null;               // IT load in MW (proxy for value at risk)
  year_built?: number | null;
  cooling_type?: "air" | "liquid" | "hybrid" | null;
  floor_level?: "above" | "basement" | "mixed" | null;

  // ── Layer 2: Hazard exposure (point-extracted, facility-level) ────────────
  // Populated by: scripts/enrich_l2_hazard.py
  jrc_flood_100yr_m?: number | null;         // JRC flood depth at 100yr return period (metres)
  usgs_pga_10pct_50yr?: number | null;       // USGS peak ground acceleration (g)
  ibtracs_track_density?: number | null;     // Tropical cyclone tracks per decade within 200 km
  wildfire_days_50km?: number | null;        // NASA FIRMS: active fire days/yr within 50 km
  heat_extreme_days?: number | null;         // ERA5: days/yr above 35°C WBGT
  // Normalised 0–100 scores derived from raw values above (legacy + updated)
  hazard_eq?: number;
  hazard_flood?: number;
  hazard_cyclone?: number;
  hazard_fire?: number;

  // ── Layer 2 (legacy): power proximity ────────────────────────────────────
  nearest_plant_km?: number | null;
  nearest_plant_fuel?: string;
  grid_score?: number;
  dc_density_50km?: number;
  concentration_score?: number;
  nat_cat_score?: number;

  // ── Layer 3: Dependency graph ─────────────────────────────────────────────
  // Populated by: scripts/enrich_l3_dependencies.py (OSM power + PeeringDB)
  substation_osm_id?: string | null;         // Nearest HV substation OSM ID
  substation_dist_km?: number | null;
  substation_cluster_id?: string | null;     // Cluster ID for shared-substation grouping
  substation_shared_count?: number | null;   // Facilities sharing the same substation
  ixp_ids?: string[] | null;                 // PeeringDB IXP IDs within 100 km
  nearest_ixp_km?: number | null;
  ixp_count_50km?: number | null;
  fibre_path_count?: number | null;          // Independent physical fibre paths to nearest IXP
  water_stress_idx?: number | null;          // FAO AQUASTAT 0–5 (5 = critically stressed)
  asn?: string | null;                       // Primary BGP ASN (PeeringDB)

  // ── Layer 4: Network topology ─────────────────────────────────────────────
  // Populated by: scripts/enrich_l4_topology.py (graph analysis)
  betweenness_centrality?: number | null;    // 0–1 normalised graph centrality
  systemic_importance_score?: number | null; // 0–100 composite systemic weight
  accumulation_flag?: boolean | null;        // True if substation cluster size ≥ 3

  // ── Layer 5: Composite risk index ────────────────────────────────────────
  risk_score?: number;                       // 0–100 weighted composite (all layers)
}

export interface PowerPlant {
  name: string;
  country: string;
  fuel_type: string;
  capacity_mw: number | null;
  owner: string;
  lat: number;
  lng: number;
}

export interface MilitaryBase {
  name: string;
  country: string;
  operator: string;
  branch: string;
  lat: number;
  lng: number;
}

// ─── NEWS / GLOBAL INCIDENTS ────────────────────────────────────────────────

export interface NewsArticle {
  id: number | string;
  title: string;
  summary: string;
  source: string;
  link: string;
  pub_date: string;
  risk_score: number;
  lat: number;
  lng: number;
  region?: string;
  coords?: [number, number];
  machine_assessment?: string;
}

// ─── UKRAINE FRONTLINE ──────────────────────────────────────────────────────

export interface FrontlineGeoJSON {
  type: "FeatureCollection";
  features: Array<{
    type: "Feature";
    geometry: {
      type: "Polygon";
      coordinates: [number, number][][];
    };
    properties: {
      name: string;
      zone_id: number;
    };
  }>;
}

// ─── GDELT INCIDENTS ────────────────────────────────────────────────────────

export interface GDELTIncident {
  type: "Feature";
  geometry: {
    type: "Point";
    coordinates: [number, number];
  };
  properties: {
    name: string;
    count: number;
    _urls_list: string[];
    _headlines_list: string[];
  };
}

// ─── LIVEUAMAP ──────────────────────────────────────────────────────────────

export interface LiveUAmapIncident {
  id: string | number;
  lat: number;
  lng: number;
  title: string;
  description?: string;
  date: string;
  timestamp?: number;
  link?: string;
  category?: string;
  region?: string;
}

// ─── STOCKS & COMMODITIES ───────────────────────────────────────────────────

export interface StockTicker {
  price: number;
  change_percent: number;
  up: boolean;
}

export type StocksData = Record<string, StockTicker>;
export type OilData = Record<string, StockTicker>;

// ─── SPACE WEATHER ──────────────────────────────────────────────────────────

export interface SpaceWeatherEvent {
  type: string;
  begin: string;
  end: string;
  classtype: string;
}

export interface SpaceWeather {
  kp_index: number | null;
  kp_text: string;
  events: SpaceWeatherEvent[];
}

// ─── WEATHER (RAINVIEWER) ───────────────────────────────────────────────────

export interface Weather {
  time: number;
  host: string;
}

// ─── AIRPORTS ───────────────────────────────────────────────────────────────

export interface Airport {
  id: string;
  name: string;
  iata: string;
  lat: number;
  lng: number;
  type: "airport";
}

// ─── RADIO FEEDS ────────────────────────────────────────────────────────────

export interface RadioFeed {
  id: string;
  name: string;
  location: string;
  category: string;
  listeners: number;
  stream_url?: string;
}

// ─── ROUTE ──────────────────────────────────────────────────────────────────

export interface FlightRoute {
  orig_loc: [number, number];
  dest_loc: [number, number];
  origin_name: string;
  dest_name: string;
}

// ─── REGION DOSSIER ─────────────────────────────────────────────────────────

export interface RegionDossier {
  lat: number;
  lng: number;
  admin_regions?: string[];
  populated_places?: string[];
  // Dynamic properties from backend (sentinel2, weather, etc.)
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  [key: string]: any;
}

// ─── FRESHNESS METADATA ─────────────────────────────────────────────────────

export type FreshnessMap = Record<string, string>;

// ─── ROOT DATA OBJECT ───────────────────────────────────────────────────────

export interface DashboardData {
  // Metadata
  last_updated?: string | null;
  freshness?: FreshnessMap;
  satellite_source?: string;

  // Fast tier
  commercial_flights?: CommercialFlight[];
  private_flights?: PrivateFlight[];
  private_jets?: PrivateJet[];
  military_flights?: MilitaryFlight[];
  tracked_flights?: TrackedFlight[];
  uavs?: UAV[];
  ships?: Ship[];
  cctv?: CCTVCamera[];
  liveuamap?: LiveUAmapIncident[];
  gps_jamming?: GPSJammingZone[];
  satellites?: Satellite[];

  // Slow tier
  news?: NewsArticle[];
  stocks?: StocksData;
  oil?: OilData;
  weather?: Weather | null;
  earthquakes?: Earthquake[];
  frontlines?: FrontlineGeoJSON | null;
  gdelt?: GDELTIncident[];
  airports?: Airport[];
  kiwisdr?: KiwiSDR[];
  space_weather?: SpaceWeather | null;
  internet_outages?: InternetOutage[];
  firms_fires?: FireHotspot[];
  datacenters?: DataCenter[];
  military_bases?: MilitaryBase[];
  power_plants?: PowerPlant[];
}

// ─── COMPONENT PROPS ────────────────────────────────────────────────────────

export interface ActiveLayers {
  flights: boolean;
  private: boolean;
  jets: boolean;
  military: boolean;
  tracked: boolean;
  satellites: boolean;
  ships_military: boolean;
  ships_cargo: boolean;
  ships_civilian: boolean;
  ships_passenger: boolean;
  ships_tracked_yachts: boolean;
  earthquakes: boolean;
  cctv: boolean;
  ukraine_frontline: boolean;
  global_incidents: boolean;
  day_night: boolean;
  gps_jamming: boolean;
  gibs_imagery: boolean;
  highres_satellite: boolean;
  kiwisdr: boolean;
  firms: boolean;
  internet_outages: boolean;
  datacenters: boolean;
  hyperscalers: boolean;
  military_bases: boolean;
  power_plants: boolean;
  power_plants_nuclear: boolean;
  power_plants_fossil: boolean;
  power_plants_renewable: boolean;
  power_plants_other: boolean;
}

export interface SelectedEntity {
  id: string | number;
  type: string;
  name?: string;
  media_url?: string;
  // Dynamic bag — varies by entity type (flight, ship, cctv, region_dossier, etc.)
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  extra?: Record<string, any>;
}

export interface MeasurePoint {
  lat: number;
  lng: number;
}

export interface MapEffects {
  bloom: boolean;
  style?: string;
}

export interface MaplibreViewerProps {
  data: DashboardData;
  activeLayers: ActiveLayers;
  activeFilters?: Record<string, string[]>;
  effects?: MapEffects;
  onEntityClick: (entity: SelectedEntity | null) => void;
  flyToLocation: { lat: number; lng: number; zoom?: number; ts?: number } | null;
  selectedEntity: SelectedEntity | null;
  onMouseCoords: (coords: { lat: number; lng: number }) => void;
  onRightClick: (coords: { lat: number; lng: number }) => void;
  regionDossier: RegionDossier | null;
  regionDossierLoading: boolean;
  onViewStateChange?: (vs: { zoom: number; latitude: number }) => void;
  measureMode: boolean;
  onMeasureClick: (coords: { lat: number; lng: number }) => void;
  measurePoints: MeasurePoint[];
  gibsDate: string;
  gibsOpacity: number;
  isEavesdropping?: boolean;
  onEavesdropClick?: (coords: { lat: number; lng: number }) => void;
  onCameraMove?: (coords: { lat: number; lng: number }) => void;
  viewBoundsRef?: React.RefObject<{ south: number; west: number; north: number; east: number } | null>;
  trackedSdr?: KiwiSDR | null;
  setTrackedSdr?: (sdr: KiwiSDR | null) => void;
}
