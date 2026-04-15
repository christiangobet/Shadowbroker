import { render, screen, fireEvent } from '@testing-library/react';
import React, { useState } from 'react';

import WorldviewLeftPanel from '@/components/WorldviewLeftPanel';
import { ThemeProvider } from '@/lib/ThemeContext';
import { mergeData } from '@/hooks/useDataStore';
import type { ActiveLayers } from '@/types/dashboard';

const storageMock = {
  getItem: () => null,
  setItem: () => {},
  removeItem: () => {},
  clear: () => {},
};

Object.defineProperty(globalThis, 'localStorage', {
  value: storageMock,
  configurable: true,
});


function buildActiveLayers(): ActiveLayers {
  return {
    flights: false,
    private: false,
    jets: false,
    military: false,
    tracked: false,
    satellites: false,
    ships_military: false,
    ships_cargo: false,
    ships_civilian: false,
    ships_passenger: false,
    ships_tracked_yachts: false,
    earthquakes: false,
    cctv: false,
    ukraine_frontline: false,
    global_incidents: false,
    day_night: false,
    gps_jamming: false,
    gibs_imagery: false,
    highres_satellite: false,
    kiwisdr: false,
    psk_reporter: false,
    satnogs: false,
    tinygs: false,
    scanners: false,
    firms: false,
    internet_outages: false,
    datacenters: false,
    hyperscalers: false,
    military_bases: false,
    power_plants: false,
    power_plants_nuclear: false,
    power_plants_fossil: false,
    power_plants_renewable: false,
    power_plants_other: false,
    sigint_meshtastic: false,
    sigint_aprs: false,
    ukraine_alerts: false,
    weather_alerts: false,
    air_quality: false,
    volcanoes: false,
    fishing_activity: false,
    sentinel_hub: false,
    trains: false,
    shodan_overlay: false,
    viirs_nightlights: false,
    correlations: false,
    dc_flood: false,
    dc_power_dependencies: false,
    dc_network_dependencies: false,
    dc_accumulation: false,
    dc_cyclone_history: false,
  };
}


function Harness() {
  const [activeLayers, setActiveLayers] = useState<ActiveLayers>(buildActiveLayers());

  return (
    <ThemeProvider>
      <WorldviewLeftPanel
        activeLayers={activeLayers}
        setActiveLayers={setActiveLayers}
        isMinimized={false}
      />
      <pre data-testid="layers-json">{JSON.stringify(activeLayers)}</pre>
    </ThemeProvider>
  );
}


describe('WorldviewLeftPanel risk layers', () => {
  it('renders underwriting risk toggles when expanded', () => {
    mergeData({ datacenters: [{ name: 'DC One', lat: 40, lng: -75 }] });
    render(<Harness />);

    fireEvent.click(screen.getByText('RISK LAYERS'));

    expect(screen.getByText('Flood Exposure')).toBeTruthy();
    expect(screen.getByText('Power Dependency')).toBeTruthy();
    expect(screen.getByText('Network Dependency')).toBeTruthy();
    expect(screen.getByText('Accumulation Clusters')).toBeTruthy();
    expect(screen.getByText('Cyclone History')).toBeTruthy();
  });

  it('toggles dc_flood state from the risk panel', () => {
    render(<Harness />);

    fireEvent.click(screen.getByText('RISK LAYERS'));
    fireEvent.click(screen.getByText('Flood Exposure'));

    expect(screen.getByTestId('layers-json').textContent).toContain('"dc_flood":true');
  });
});
