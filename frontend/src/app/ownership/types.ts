// ---------------------------------------------------------------------------
// Types matching the /api/v2/ownership/* responses
// (backend/app/api/ownership.py — stats, scheme/{id}, targets)
// ---------------------------------------------------------------------------

export interface OwnershipTypeCount {
  type: string;
  companies: number;
  schemes: number;
}

export interface OwnershipStats {
  companies_walked: number;
  spv_candidates: number;
  platform_clusters: number;
  by_type: OwnershipTypeCount[];
}

export interface OwnershipChainNode {
  level: number;
  name: string;
  kind: string; // 'corporate' | 'individual' | 'statement'
  ch_number: string | null;
  country: string | null;
}

export interface SchemeOwnership {
  scheme_id: number;
  scheme_name: string;
  owner_company_id: number | null;
  owner_name: string | null;
  owner_ch_number: string | null;
  is_spv_candidate: boolean | null;
  ultimate_owner_name: string | null;
  ultimate_owner_type: string | null;
  registered_office: string | null;
  chain: OwnershipChainNode[];
}

export interface OwnershipTarget {
  target: string;
  owner_type: string | null;
  vehicles: number;
  spv_count: number;
  schemes: number;
  units: number;
  councils: string[];
  max_arrears: number | null;
  vehicle_names: string[];
}

export interface OwnershipTargetsResponse {
  targets: OwnershipTarget[];
  count: number;
}

// ---------------------------------------------------------------------------
// Shared helpers — owner-type badge tones used on /ownership and in the
// schemes ownership panel.
// ---------------------------------------------------------------------------

export type OwnerTypeTone = 'red' | 'blue' | 'amber' | 'violet' | 'slate';

export function ownerTypeTone(type?: string | null): OwnerTypeTone {
  const t = (type || '').toUpperCase();
  if (t.includes('OFFSHORE')) return 'violet';
  if (/\bPE\b/.test(t) || t.includes('FUND') || t.includes('INSTITUTIONAL')) return 'red';
  if (t.includes('PRIVATE') || t.includes('INDIVIDUAL')) return 'amber';
  if (t.includes('CORPORATE') || t.includes('GROUP')) return 'blue';
  return 'slate';
}

export const OWNER_TYPE_BADGE: Record<OwnerTypeTone, string> = {
  red: 'bg-red-500/15 text-red-300 border-red-500/40',
  blue: 'bg-blue-500/15 text-blue-300 border-blue-500/40',
  amber: 'bg-amber-500/15 text-amber-300 border-amber-500/30',
  violet: 'bg-violet-500/15 text-violet-300 border-violet-500/40',
  slate: 'bg-slate-700/50 text-slate-400 border-slate-600',
};

export const OWNER_TYPE_BAR: Record<OwnerTypeTone, string> = {
  red: 'bg-red-500',
  blue: 'bg-blue-500',
  amber: 'bg-amber-500',
  violet: 'bg-violet-500',
  slate: 'bg-slate-500',
};
