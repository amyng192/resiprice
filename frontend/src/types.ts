export interface Unit {
  unit_number: string;
  floor_plan_name: string | null;
  unit_type: string | null;
  bedrooms: number | null;
  bathrooms: number | null;
  sqft: number | null;
  rent_min: number | null;
  rent_max: number | null;
  deposit: number | null;
  available_date: string | null;
  status: string;
}

export interface Property {
  name: string;
  address: string | null;
  platform: string | null;
  website_url: string | null;
  units: Unit[];
  unit_mix: Record<string, number>;
  available_count: number;
  specials: { description: string }[];
}

export type ScrapeStatus = "idle" | "scraping" | "done" | "error";

export interface PropertySlot {
  url: string;
  status: ScrapeStatus;
  property: Property | null;
  error: string | null;
}
