'use client';

import React, { useState, useEffect } from 'react';
import {
  ChevronDownIcon,
  ChevronUpIcon,
  BuildingOffice2Icon,
  HomeModernIcon,
  ChartBarSquareIcon,
  ExclamationTriangleIcon,
  ArrowTrendingUpIcon,
  ArrowTrendingDownIcon,
  SparklesIcon,
  PlusCircleIcon,
  CheckCircleIcon,
  ShareIcon,
} from '@heroicons/react/24/outline';
import { cn, formatDate, getSchemeTypeColor, getPriorityColor, getBdScoreColor, getBdScoreBarColor, getContractEndColor, formatNumber } from '@/lib/utils';
import Card from '@/components/ui/Card';
import Badge from '@/components/ui/Badge';
import SearchInput from '@/components/ui/SearchInput';
import Select from '@/components/ui/Select';
import Modal from '@/components/ui/Modal';
import PermissionGate from '@/components/rbac/PermissionGate';
import AIEnrichPanel from '@/components/AIEnrichPanel';
import InlineFieldEdit from '@/components/InlineFieldEdit';
import FilterPanel, { SchemeFilters, DEFAULT_FILTERS, countActiveFilters } from '@/components/schemes/FilterPanel';
import ActiveFilterPills from '@/components/schemes/ActiveFilterPills';
import { getSchemesFilterOptions, SchemesFilterOptions } from '@/lib/api';
import { SchemeOwnership, ownerTypeTone, OWNER_TYPE_BADGE, OWNER_TYPE_BAR } from '@/app/ownership/types';
import Link from 'next/link';
import { useRouter, useSearchParams } from 'next/navigation';
import { AdjustmentsHorizontalIcon } from '@heroicons/react/24/outline';
import toast from 'react-hot-toast';
import api from '@/lib/api';

interface SchemeRow {
  id: string;
  name: string;
  operator: string;
  council: string;
  units: number | null;
  contract_end: string;
  performance: number | null;
  satisfaction: number | null;
  bd_score: number | null;
  arrears_risk_score: number | null;  // 0-100, higher = more operator distress
  priority: 'high' | 'medium' | 'low';
  scheme_type: string;
  address: string;
  postcode: string;
  owner: string;
  asset_manager: string;
  landlord: string;
  contract_start: string;
  occupancy_rate: number | null;
  revenue_per_unit: number | null;
  score_breakdown: {
    contract_proximity: number;
    performance_gap: number;
    market_opportunity: number;
    relationship_strength: number;
    scheme_size: number;
  };
  operator_company_id: string | null;
  pipeline_opportunity_id: string | null;
  locked_fields: Record<string, string>;
  min_rent_per_week: number | null;
  rent_tier_count: number;
  region: string | null;
}

// Rent timeline component for scheme details
interface RentRecord {
  id: string;
  room_type: string | null;
  rent_per_week: number | null;
  rent_per_month: number | null;
  currency: string;
  academic_year: string | null;
  contract_length_weeks: number | null;
  source: string | null;
}

function RentPanel({ schemeId }: { schemeId: string }) {
  const [rents, setRents] = React.useState<RentRecord[]>([]);
  const [loading, setLoading] = React.useState(true);

  React.useEffect(() => {
    api.get(`/v2/schemes/${schemeId}/rents`)
      .then(res => {
        setRents(res.data || []);
        setLoading(false);
      })
      .catch(() => setLoading(false));
  }, [schemeId]);

  if (loading) return <div className="mt-4 text-xs text-slate-500">Loading rents...</div>;
  if (rents.length === 0) return null;

  // Group by academic year; keep null-year group last
  const byYear = new Map<string, RentRecord[]>();
  for (const r of rents) {
    const key = r.academic_year || 'Unknown';
    if (!byYear.has(key)) byYear.set(key, []);
    byYear.get(key)!.push(r);
  }
  const years = Array.from(byYear.keys()).sort((a, b) => {
    if (a === 'Unknown') return 1;
    if (b === 'Unknown') return -1;
    return b.localeCompare(a);
  });

  return (
    <div className="mt-6">
      <h4 className="text-sm font-semibold text-white mb-3 flex items-center gap-2">
        <span className="w-1 h-4 bg-amber-500 rounded-full" />
        Rent Tiers ({rents.length})
      </h4>
      <div className="space-y-4">
        {years.map(year => {
          const yearRents = byYear.get(year)!
            .slice()
            .sort((a, b) => (a.rent_per_week ?? Infinity) - (b.rent_per_week ?? Infinity));
          return (
            <div key={year}>
              <div className="flex items-center gap-2 mb-2">
                <span className="text-xs font-medium text-slate-400 uppercase tracking-wide">
                  {year === 'Unknown' ? 'Unspecified year' : `Academic year ${year}`}
                </span>
                <span className="text-[10px] text-slate-600">· {yearRents.length} tier{yearRents.length !== 1 ? 's' : ''}</span>
              </div>
              <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-2">
                {yearRents.map(r => (
                  <div
                    key={r.id}
                    className="flex items-center justify-between rounded-lg bg-slate-700/30 border border-slate-700/50 px-3 py-2"
                  >
                    <div className="min-w-0 flex-1">
                      <p className="text-xs font-medium text-white truncate">
                        {r.room_type || 'Room'}
                      </p>
                      {r.contract_length_weeks && (
                        <p className="text-[10px] text-slate-500">
                          {r.contract_length_weeks}-week tenancy
                        </p>
                      )}
                    </div>
                    <div className="flex-shrink-0 text-right ml-3">
                      {r.rent_per_week != null && (
                        <div className="text-sm font-semibold text-amber-300">
                          £{r.rent_per_week.toFixed(0)}
                          <span className="text-[10px] text-slate-500 font-normal">/wk</span>
                        </div>
                      )}
                      {r.rent_per_month != null && (
                        <div className="text-sm font-semibold text-amber-300">
                          £{r.rent_per_month.toFixed(0)}
                          <span className="text-[10px] text-slate-500 font-normal">/mo</span>
                        </div>
                      )}
                      {r.source && (
                        <div className="text-[9px] text-slate-600 mt-0.5">{r.source}</div>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}


// Contract timeline component for scheme details
interface ContractRecord {
  id: string;
  contract_reference: string | null;
  contract_type: string | null;
  operator: string | null;
  client: string | null;
  contract_start: string | null;
  contract_end: string | null;
  contract_value: number | null;
  source: string | null;
  is_current: boolean | null;
}

function ContractTimeline({ schemeId }: { schemeId: string }) {
  const [contracts, setContracts] = React.useState<ContractRecord[]>([]);
  const [loading, setLoading] = React.useState(true);

  React.useEffect(() => {
    api.get(`/v2/schemes/${schemeId}/contracts`)
      .then(res => {
        setContracts(res.data || []);
        setLoading(false);
      })
      .catch(() => setLoading(false));
  }, [schemeId]);

  if (loading) return <div className="mt-4 text-xs text-slate-500">Loading contracts...</div>;
  if (contracts.length === 0) return null;

  return (
    <div className="mt-6">
      <h4 className="text-sm font-semibold text-white mb-3 flex items-center gap-2">
        <span className="w-1 h-4 bg-emerald-500 rounded-full" />
        Contract History ({contracts.length})
      </h4>
      <div className="space-y-2">
        {contracts.map((c) => {
          const isCurrent = c.is_current;
          const isExpired = c.contract_end && new Date(c.contract_end) < new Date();
          return (
            <div
              key={c.id}
              className={cn(
                'flex items-center gap-4 rounded-lg px-4 py-3 text-xs border',
                isCurrent
                  ? 'bg-emerald-500/10 border-emerald-500/30'
                  : isExpired
                  ? 'bg-slate-700/30 border-slate-700/50'
                  : 'bg-blue-500/10 border-blue-500/30'
              )}
            >
              <div className="flex-shrink-0">
                <div className={cn(
                  'w-2 h-2 rounded-full',
                  isCurrent ? 'bg-emerald-400' : isExpired ? 'bg-slate-500' : 'bg-blue-400'
                )} />
              </div>
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2">
                  <span className="font-medium text-white truncate">{c.contract_type || 'Contract'}</span>
                  {isCurrent && <span className="text-[10px] bg-emerald-500/20 text-emerald-400 px-1.5 py-0.5 rounded">Current</span>}
                  {isExpired && <span className="text-[10px] bg-slate-600/50 text-slate-400 px-1.5 py-0.5 rounded">Expired</span>}
                </div>
                <div className="flex items-center gap-3 mt-1 text-slate-400">
                  {c.operator && <span>Operator: <span className="text-slate-300">{c.operator}</span></span>}
                  {c.client && <span>Client: <span className="text-slate-300">{c.client}</span></span>}
                </div>
              </div>
              <div className="flex-shrink-0 text-right">
                <div className="text-slate-300">
                  {c.contract_start ? formatDate(c.contract_start) : '?'} — {c.contract_end ? formatDate(c.contract_end) : '?'}
                </div>
                {c.contract_value && (
                  <div className="text-slate-400 mt-0.5">£{formatNumber(c.contract_value)}</div>
                )}
              </div>
              {c.source && (
                <div className="flex-shrink-0">
                  <span className="text-[10px] bg-slate-700 text-slate-400 px-1.5 py-0.5 rounded">{c.source}</span>
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}

// BD Score Breakdown — calibrated, labelled bars with tooltips
interface ScoreDim {
  key: keyof SchemeRow['score_breakdown'];
  label: string;
  tooltip: string;
  // Is this dimension populated for the current scheme? (used to show "No data" rather than the default 50)
  hasDataFor: (s: SchemeRow) => boolean;
  // Direction: "higher is more BD opportunity" (red=high) vs "higher is better for us" (green=high)
  direction: 'opportunity' | 'strength';
}

const SCORE_DIMENSIONS: ScoreDim[] = [
  {
    key: 'contract_proximity',
    label: 'Contract proximity',
    tooltip: 'How soon the current contract ends. Higher = closer to renewal = bigger BD opportunity.',
    hasDataFor: (s) => Boolean(s.contract_end),
    direction: 'opportunity',
  },
  {
    key: 'performance_gap',
    label: 'Performance gap',
    tooltip: 'Inverse of the current operator\'s performance rating. Higher = operator underperforming = BD opportunity.',
    hasDataFor: (s) => s.performance !== null,
    direction: 'opportunity',
  },
  {
    key: 'market_opportunity',
    label: 'Market opportunity',
    tooltip: 'Inverse of resident satisfaction. Higher = unhappy residents = BD opportunity.',
    hasDataFor: (s) => s.satisfaction !== null,
    direction: 'opportunity',
  },
  {
    key: 'relationship_strength',
    label: 'Financial health',
    tooltip: 'Operator\'s financial health score. Higher = healthier (less risky relationship).',
    hasDataFor: (s) => s.score_breakdown.relationship_strength !== 50,
    direction: 'strength',
  },
  {
    key: 'scheme_size',
    label: 'Scheme size',
    tooltip: 'Derived from unit count (>500 = 100, >200 = 70, >100 = 50, else 30). Bigger scheme = bigger BD prize.',
    hasDataFor: (s) => s.units !== null,
    direction: 'opportunity',
  },
];

function barColor(val: number, direction: 'opportunity' | 'strength'): string {
  // For "opportunity" dims: high (red) = big opportunity for us.
  // For "strength"  dims: high (green) = strong position.
  if (direction === 'opportunity') {
    if (val > 70) return 'bg-gradient-to-r from-red-600 to-red-400';
    if (val > 40) return 'bg-gradient-to-r from-amber-600 to-amber-400';
    return 'bg-gradient-to-r from-slate-600 to-slate-500';
  }
  if (val > 70) return 'bg-gradient-to-r from-emerald-600 to-emerald-400';
  if (val > 40) return 'bg-gradient-to-r from-amber-600 to-amber-400';
  return 'bg-gradient-to-r from-red-600 to-red-400';
}

interface Competitor {
  operator_id: number;
  operator_name: string;
  scheme_count: number;
  avg_units: number | null;
  has_rent_data: boolean;
  sample_scheme_name: string | null;
  sample_scheme_id: string | null;
}

function CompetitorPanel({
  scheme,
  onFilterBy,
}: {
  scheme: SchemeRow;
  onFilterBy: (c: Competitor) => void;
}) {
  const [competitors, setCompetitors] = React.useState<Competitor[]>([]);
  const [loading, setLoading] = React.useState(true);

  React.useEffect(() => {
    api.get(`/v2/schemes/${scheme.id}/competitors`)
      .then(res => { setCompetitors(res.data || []); setLoading(false); })
      .catch(() => setLoading(false));
  }, [scheme.id]);

  return (
    <div className="space-y-4">
      <h4 className="text-sm font-semibold text-white flex items-center gap-2">
        <span className="w-1 h-4 bg-amber-500 rounded-full" />
        Competing Operators
        <span className="text-[10px] text-slate-500 font-normal ml-1">
          {scheme.scheme_type} in {scheme.region || scheme.council || 'this area'}
        </span>
      </h4>
      {loading && (
        <div className="text-xs text-slate-500">Loading competitors...</div>
      )}
      {!loading && competitors.length === 0 && (
        <div className="text-xs text-slate-500 italic">
          No other operators found for {scheme.scheme_type} schemes in this region.
        </div>
      )}
      {!loading && competitors.length > 0 && (
        <div className="space-y-2">
          <p className="text-[10px] text-slate-500 italic">Click an operator to filter schemes by it</p>
          {competitors.map((c) => (
            <button
              key={c.operator_id}
              type="button"
              onClick={(e) => { e.stopPropagation(); onFilterBy(c); }}
              className="w-full text-left bg-slate-700/30 rounded-lg p-3 border border-slate-700/50 hover:bg-slate-700/60 hover:border-amber-500/40 transition-colors group"
            >
              <div className="flex items-center justify-between gap-2">
                <p className="text-xs font-semibold text-white truncate group-hover:text-amber-200">{c.operator_name}</p>
                <div className="flex-shrink-0 flex items-center gap-1.5">
                  {c.has_rent_data && (
                    <span className="text-[9px] bg-amber-500/15 text-amber-300 border border-amber-500/30 px-1.5 py-0.5 rounded-full">£ rent</span>
                  )}
                  <span className="text-[10px] bg-slate-800 text-slate-400 px-1.5 py-0.5 rounded">
                    {c.scheme_count} {c.scheme_count === 1 ? 'scheme' : 'schemes'}
                  </span>
                </div>
              </div>
              <div className="flex items-center gap-3 mt-1.5 text-[10px] text-slate-500">
                {c.avg_units !== null && (
                  <span>Avg {Math.round(c.avg_units)} units/scheme</span>
                )}
                {c.sample_scheme_name && (
                  <span className="truncate">e.g. {c.sample_scheme_name}</span>
                )}
              </div>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}


// Operator Health Panel — surfaces the arrears risk score and its meaning
// for the scheme's operator. Pulls the score (0-100) from the scheme row
// and decodes the dominant Companies House signal cluster.
function OperatorHealthPanel({ scheme }: { scheme: SchemeRow }) {
  const score = scheme.arrears_risk_score;
  const lastChecked = (scheme as any).arrears_checked_at as string | null | undefined;

  if (score === null || score === undefined) {
    return (
      <div className="mt-6 rounded-lg border border-slate-700/40 bg-slate-800/30 p-4">
        <div className="flex items-center gap-2 mb-1">
          <ExclamationTriangleIcon className="w-4 h-4 text-slate-500" />
          <h4 className="text-sm font-semibold text-slate-300">Operator Health</h4>
        </div>
        <p className="text-xs text-slate-500">
          No arrears data yet. Score is computed from the operator&apos;s Companies House
          profile (status, overdue filings, recent charges). Coverage builds as company
          lookups complete.
        </p>
      </div>
    );
  }

  const bucket = score >= 80 ? 'critical' : score >= 60 ? 'distressed' : score >= 35 ? 'caution' : 'healthy';
  const bucketMeta: Record<string, { label: string; cls: string; barCls: string; signal: string }> = {
    critical: {
      label: 'Critical',
      cls: 'bg-red-500/20 text-red-300 border-red-500/40',
      barCls: 'bg-red-500',
      signal: 'Multiple severe signals — likely dissolved, in administration, or with multiple overdue filings',
    },
    distressed: {
      label: 'Distressed',
      cls: 'bg-orange-500/20 text-orange-300 border-orange-500/40',
      barCls: 'bg-orange-500',
      signal: 'Multiple concurrent signals — overdue filings + recent debenture or serial refinancing',
    },
    caution: {
      label: 'Caution',
      cls: 'bg-amber-500/15 text-amber-300 border-amber-500/30',
      barCls: 'bg-amber-500',
      signal: 'One mild signal — typically a recent charge or stalled filing',
    },
    healthy: {
      label: 'Healthy',
      cls: 'bg-slate-700/50 text-slate-300 border-slate-600',
      barCls: 'bg-emerald-500/60',
      signal: 'No distress signals on operator’s Companies House profile',
    },
  };
  const meta = bucketMeta[bucket];

  return (
    <div className="mt-6 rounded-lg border border-slate-700/40 bg-slate-800/30 overflow-hidden">
      {/* Header */}
      <div className="flex items-center justify-between px-5 py-3 border-b border-slate-700/40">
        <div className="flex items-center gap-2">
          <ExclamationTriangleIcon className="w-4 h-4 text-amber-400" />
          <h4 className="text-sm font-semibold text-white">Operator Health</h4>
          <span className="text-xs text-slate-500">·</span>
          <span className="text-xs text-slate-400">{scheme.operator || 'Unknown operator'}</span>
        </div>
        {lastChecked && (
          <span className="text-[11px] text-slate-500">
            Last check: {new Date(lastChecked).toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' })}
          </span>
        )}
      </div>

      {/* Body */}
      <div className="p-5 grid grid-cols-1 lg:grid-cols-3 gap-5">
        {/* Score chip */}
        <div className="flex flex-col items-center justify-center py-3">
          <div className={cn(
            'w-24 h-24 rounded-full flex items-center justify-center border-4',
            bucket === 'critical' && 'border-red-500/60',
            bucket === 'distressed' && 'border-orange-500/60',
            bucket === 'caution' && 'border-amber-500/60',
            bucket === 'healthy' && 'border-emerald-500/40',
          )}>
            <div className="text-center">
              <div className="text-2xl font-bold text-white tabular-nums">{score.toFixed(0)}</div>
              <div className="text-[10px] text-slate-500 uppercase tracking-wide">/ 100</div>
            </div>
          </div>
          <span className={cn('mt-3 inline-flex items-center gap-1 px-2 py-0.5 rounded-md text-xs font-semibold border', meta.cls)}>
            {meta.label}
          </span>
        </div>

        {/* Signal interpretation */}
        <div className="lg:col-span-2">
          <div className="text-xs text-slate-500 uppercase tracking-wide mb-2">What this means</div>
          <p className="text-sm text-slate-300 mb-4 leading-relaxed">{meta.signal}</p>

          <div className="text-xs text-slate-500 uppercase tracking-wide mb-2">How the score is built</div>
          <div className="space-y-1.5">
            {[
              { label: 'Status: dissolved / liquidation / admin', points: '+40', alwaysVisible: score >= 70 },
              { label: 'Accounts overdue', points: '+25', alwaysVisible: score >= 55 },
              { label: 'Confirmation statement overdue', points: '+15', alwaysVisible: score >= 45 },
              { label: 'Recent debenture (last 6 months)', points: '+15', alwaysVisible: score >= 45 },
              { label: 'Serial refinancing pattern', points: '+15', alwaysVisible: score >= 60 },
              { label: 'Baseline (no signals)', points: '30', alwaysVisible: true },
            ].map((row) => (
              <div key={row.label} className={cn(
                'flex items-center justify-between text-xs px-2 py-1 rounded',
                row.alwaysVisible ? 'bg-slate-700/30' : 'opacity-50',
              )}>
                <span className={cn('text-slate-300', row.alwaysVisible && score >= 60 && 'text-amber-300')}>
                  {row.alwaysVisible && score >= 60 && (score === 30 ? '' : '✓ ')}{row.label}
                </span>
                <span className="font-mono text-slate-500">{row.points}</span>
              </div>
            ))}
          </div>
          <p className="mt-3 text-[11px] text-slate-500 italic">
            Score is a heuristic from Companies House public data. Higher = more operator distress = stronger BD opportunity.
            <Link href="/arrears" className="ml-1 text-amber-400 hover:text-amber-300">View all distressed schemes →</Link>
          </p>
        </div>
      </div>
    </div>
  );
}

// Ownership chain panel — scheme → owner SPV → platform → ultimate owner.
// Lazy: only mounted when the scheme row is expanded, fetching
// /api/v2/ownership/scheme/{id} on first render.
const UK_COUNTRY_NAMES = new Set([
  'united kingdom', 'uk', 'great britain', 'england', 'wales', 'scotland',
  'northern ireland', 'england and wales',
]);

function isNonUkCountry(country?: string | null): boolean {
  if (!country) return false;
  return !UK_COUNTRY_NAMES.has(country.trim().toLowerCase());
}

function chainKindBadge(kind: string) {
  const k = kind.toLowerCase();
  const cls = k.includes('individual')
    ? 'bg-amber-500/15 text-amber-300 border-amber-500/30'
    : 'bg-blue-500/15 text-blue-300 border-blue-500/40';
  const label = k.includes('individual') ? 'Individual' : 'Corporate';
  return (
    <span className={cn('text-[9px] px-1.5 py-0.5 rounded border uppercase tracking-wide font-semibold', cls)}>
      {label}
    </span>
  );
}

interface OwnershipDisplayNode {
  name: string;
  kind: string;
  ch_number: string | null;
  country: string | null;
  isOwner?: boolean;
  isUltimate?: boolean;
}

function OwnershipPanel({ schemeId }: { schemeId: string }) {
  const [data, setData] = React.useState<SchemeOwnership | null>(null);
  const [loading, setLoading] = React.useState(true);

  React.useEffect(() => {
    api.get(`/v2/ownership/scheme/${schemeId}`)
      .then(res => { setData(res.data); setLoading(false); })
      .catch(() => setLoading(false));
  }, [schemeId]);

  if (loading) return <div className="mt-6 text-xs text-slate-500">Loading ownership chain...</div>;
  if (!data) return null;

  // No owner linked yet — HMLR title data hasn't been ingested for this scheme.
  if (!data.owner_company_id) {
    return (
      <div className="mt-6 rounded-lg border border-slate-700/40 bg-slate-800/30 p-4">
        <div className="flex items-center gap-2 mb-1">
          <ShareIcon className="w-4 h-4 text-slate-500" />
          <h4 className="text-sm font-semibold text-slate-300">Ownership</h4>
        </div>
        <p className="text-xs text-slate-500">No owner recorded — HMLR ingest pending.</p>
      </div>
    );
  }

  const ownerName = (data.owner_name || '').toLowerCase();
  const chain = data.chain || [];
  // 'statement' nodes are CH declarations (e.g. no-PSC), not entities
  const statements = chain.filter((n) => n.kind === 'statement');
  const entities = chain.filter(
    (n) => n.kind !== 'statement' && n.name.toLowerCase() !== ownerName,
  );

  const nodes: OwnershipDisplayNode[] = [
    {
      name: data.owner_name || 'Unknown owner',
      kind: 'corporate',
      ch_number: data.owner_ch_number,
      country: null,
      isOwner: true,
    },
    ...entities.map((n) => ({
      name: n.name,
      kind: n.kind,
      ch_number: n.ch_number,
      country: n.country,
    })),
  ];
  const ultimateName = (data.ultimate_owner_name || '').toLowerCase();
  if (ultimateName) {
    const match = nodes.find((n) => n.name.toLowerCase() === ultimateName);
    if (match) {
      match.isUltimate = true;
    } else {
      nodes.push({
        name: data.ultimate_owner_name!,
        kind: 'corporate',
        ch_number: null,
        country: null,
        isUltimate: true,
      });
    }
  }

  const typeTone = ownerTypeTone(data.ultimate_owner_type);

  return (
    <div className="mt-6 rounded-lg border border-slate-700/40 bg-slate-800/30 overflow-hidden">
      {/* Header */}
      <div className="flex items-center justify-between px-5 py-3 border-b border-slate-700/40 flex-wrap gap-2">
        <div className="flex items-center gap-2">
          <ShareIcon className="w-4 h-4 text-violet-400" />
          <h4 className="text-sm font-semibold text-white">Ownership Chain</h4>
          {data.is_spv_candidate && (
            <span className="text-[9px] px-1.5 py-0.5 rounded border uppercase tracking-wide font-semibold bg-violet-500/15 text-violet-300 border-violet-500/40">
              SPV
            </span>
          )}
        </div>
        {data.ultimate_owner_type && (
          <span className={cn('inline-flex items-center px-2 py-0.5 rounded-md text-[11px] font-semibold border', OWNER_TYPE_BADGE[typeTone])}>
            {data.ultimate_owner_type}
          </span>
        )}
      </div>

      {/* Vertical breadcrumb */}
      <div className="p-5">
        <div>
          {nodes.map((n, i) => (
            <div key={`${n.name}-${i}`} className="flex gap-3">
              {/* Connector column */}
              <div className="flex flex-col items-center">
                <div
                  className={cn(
                    'w-2.5 h-2.5 rounded-full mt-1 flex-shrink-0',
                    n.isUltimate ? OWNER_TYPE_BAR[typeTone] : n.isOwner ? 'bg-violet-500' : 'bg-slate-600',
                  )}
                />
                {i < nodes.length - 1 && <div className="w-px flex-1 bg-slate-700 my-1" />}
              </div>
              {/* Node */}
              <div className={cn('min-w-0', i < nodes.length - 1 ? 'pb-4' : 'pb-0')}>
                <div className="flex items-center gap-2 flex-wrap">
                  <span className={cn('text-sm font-medium', n.isUltimate ? 'text-white' : 'text-slate-200')}>
                    {n.name}
                  </span>
                  {chainKindBadge(n.kind)}
                  {n.isOwner && (
                    <span className="text-[9px] px-1.5 py-0.5 rounded border uppercase tracking-wide font-semibold bg-slate-700/50 text-slate-400 border-slate-600">
                      Owner
                    </span>
                  )}
                  {n.isOwner && data.is_spv_candidate && (
                    <span className="text-[9px] px-1.5 py-0.5 rounded border uppercase tracking-wide font-semibold bg-violet-500/15 text-violet-300 border-violet-500/40">
                      SPV
                    </span>
                  )}
                  {n.isUltimate && (
                    <span className={cn('text-[9px] px-1.5 py-0.5 rounded border uppercase tracking-wide font-semibold', OWNER_TYPE_BADGE[typeTone])}>
                      Ultimate owner
                    </span>
                  )}
                  {isNonUkCountry(n.country) && (
                    <span className="text-[10px] text-violet-300 uppercase tracking-wide">{n.country}</span>
                  )}
                </div>
                <div className="flex items-center gap-3 mt-0.5">
                  {n.ch_number && (
                    <span className="text-[10px] font-mono text-slate-500">CH {n.ch_number}</span>
                  )}
                  {n.isOwner && data.registered_office && (
                    <span className="text-[10px] text-slate-500 truncate">{data.registered_office}</span>
                  )}
                </div>
              </div>
            </div>
          ))}
        </div>

        {statements.length > 0 && (
          <p className="mt-3 text-[10px] text-slate-500 italic">
            {statements.map((s) => s.name.replace(/-/g, ' ')).join(' · ')}
          </p>
        )}
        {entities.length === 0 && !data.ultimate_owner_name && (
          <p className="mt-3 text-[10px] text-slate-500 italic">PSC chain not walked yet.</p>
        )}
      </div>
    </div>
  );
}

function BDScoreBreakdown({ scheme }: { scheme: SchemeRow }) {
  const breakdown = scheme.score_breakdown;
  return (
    <div className="space-y-4">
      <h4 className="text-sm font-semibold text-white flex items-center gap-2">
        <span className="w-1 h-4 bg-violet-500 rounded-full" />
        BD Score Breakdown
      </h4>
      <div className="space-y-2.5">
        {SCORE_DIMENSIONS.map((dim) => {
          const val = breakdown[dim.key] as number;
          const missing = !dim.hasDataFor(scheme);
          const displayVal = missing ? null : Math.round(val);
          return (
            <div key={dim.key} title={dim.tooltip}>
              <div className="flex items-center justify-between mb-1">
                <span className="text-[11px] text-slate-400 flex items-center gap-1 cursor-help">
                  {dim.label}
                  <span className="text-slate-600">ⓘ</span>
                </span>
                <span className="text-[11px] font-semibold text-slate-300">
                  {displayVal === null ? <span className="text-slate-600 italic font-normal">No data</span> : `${displayVal}/100`}
                </span>
              </div>
              <div className="h-1.5 bg-slate-700 rounded-full overflow-hidden">
                {displayVal !== null && (
                  <div
                    className={cn('h-full rounded-full transition-all duration-700', barColor(val, dim.direction))}
                    style={{ width: `${val}%` }}
                  />
                )}
              </div>
            </div>
          );
        })}
      </div>
      <p className="text-[10px] text-slate-500 leading-relaxed pt-1 border-t border-slate-700/50">
        Overall BD score weights: Contract proximity 35%, Performance gap 25%, Market opportunity 15%,
        Financial health 15%, Scheme size 10%.
      </p>
    </div>
  );
}

// Ring/gauge component for performance and satisfaction
function GaugeRing({ value, size = 36, strokeWidth = 3, color }: { value: number | null; size?: number; strokeWidth?: number; color: string }) {
  const radius = (size - strokeWidth) / 2;
  const circumference = 2 * Math.PI * radius;
  const safeValue = value ?? 0;
  const offset = circumference - (safeValue / 100) * circumference;
  const hasValue = value !== null && value !== undefined;

  return (
    <div className="relative inline-flex items-center justify-center" style={{ width: size, height: size }}>
      <svg width={size} height={size} className="-rotate-90">
        <circle cx={size / 2} cy={size / 2} r={radius} stroke="currentColor" className="text-slate-700" strokeWidth={strokeWidth} fill="none" />
        {hasValue && (
          <circle
            cx={size / 2} cy={size / 2} r={radius}
            stroke={color}
            strokeWidth={strokeWidth}
            fill="none"
            strokeDasharray={circumference}
            strokeDashoffset={offset}
            strokeLinecap="round"
            className="transition-all duration-700"
          />
        )}
      </svg>
      <span className="absolute text-[10px] font-bold text-slate-300">{hasValue ? safeValue : '--'}</span>
    </div>
  );
}

// Radial BD Score display
function BdScoreRadial({ score }: { score: number | null }) {
  const size = 44;
  const strokeWidth = 4;
  const radius = (size - strokeWidth) / 2;
  const circumference = 2 * Math.PI * radius;
  const safeScore = score ?? 0;
  const offset = circumference - (safeScore / 100) * circumference;
  const color = safeScore > 80 ? '#ef4444' : safeScore > 50 ? '#f59e0b' : '#22c55e';
  const hasScore = score !== null && score !== undefined;

  return (
    <div className="relative inline-flex items-center justify-center" style={{ width: size, height: size }}>
      <svg width={size} height={size} className="-rotate-90">
        <circle cx={size / 2} cy={size / 2} r={radius} stroke="currentColor" className="text-slate-700" strokeWidth={strokeWidth} fill="none" />
        {hasScore && (
          <circle
            cx={size / 2} cy={size / 2} r={radius}
            stroke={color}
            strokeWidth={strokeWidth}
            fill="none"
            strokeDasharray={circumference}
            strokeDashoffset={offset}
            strokeLinecap="round"
            className="transition-all duration-700"
          />
        )}
      </svg>
      <span className={cn('absolute text-xs font-bold', hasScore ? getBdScoreColor(safeScore) : 'text-slate-500')}>
        {hasScore ? Math.round(safeScore) : '--'}
      </span>
    </div>
  );
}

function getDaysRemaining(dateStr: string): number | null {
  if (!dateStr) return null;
  const now = new Date();
  const end = new Date(dateStr);
  if (isNaN(end.getTime())) return null;
  return Math.ceil((end.getTime() - now.getTime()) / (1000 * 60 * 60 * 24));
}

function getDaysRemainingColor(days: number): string {
  if (days < 90) return 'text-red-400';
  if (days < 180) return 'text-amber-400';
  if (days < 365) return 'text-yellow-300';
  return 'text-slate-400';
}

function getGaugeColor(value: number | null): string {
  if (value === null || value === undefined) return '#475569';
  if (value >= 80) return '#22c55e';
  if (value >= 70) return '#84cc16';
  if (value >= 60) return '#f59e0b';
  return '#ef4444';
}

const PRIORITY_TO_BACKEND: Record<string, string> = { high: 'hot', medium: 'warm', low: 'cold' };

function AddToPipelineModal({ scheme, isOpen, onClose, onSuccess }: {
  scheme: SchemeRow | null;
  isOpen: boolean;
  onClose: () => void;
  onSuccess: (schemeId: string, opportunityId: string) => void;
}) {
  const [assignedTo, setAssignedTo] = useState('');
  const [notes, setNotes] = useState('');
  const [priority, setPriority] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState('');

  useEffect(() => {
    if (scheme) {
      setPriority(scheme.priority || 'low');
      setAssignedTo('');
      setNotes('');
      setError('');
    }
  }, [scheme]);

  if (!scheme) return null;

  const handleSubmit = async () => {
    if (!scheme.operator_company_id) return;
    setSubmitting(true);
    setError('');
    try {
      const res = await api.post('/pipeline', {
        source: 'existing_scheme',
        scheme_id: parseInt(scheme.id),
        company_id: parseInt(scheme.operator_company_id),
        bd_score: scheme.bd_score,
        stage: 'identified',
        priority: PRIORITY_TO_BACKEND[priority] || 'warm',
        assigned_to: assignedTo || null,
        notes: notes || null,
      });
      toast.success(
        <span>Added to pipeline. <a href="/pipeline" className="underline font-semibold">View Pipeline</a></span>
      );
      onSuccess(scheme.id, String(res.data.id));
      onClose();
    } catch (err: any) {
      if (err.response?.status === 409) {
        setError('This scheme is already in the pipeline.');
      } else {
        setError(err.response?.data?.detail || 'Failed to add to pipeline.');
      }
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <Modal isOpen={isOpen} onClose={onClose} title="Add to Pipeline" size="md">
      <div className="space-y-5">
        {/* Summary */}
        <div className="bg-slate-700/30 rounded-lg p-4 space-y-2">
          <p className="text-sm font-semibold text-white">{scheme.name}</p>
          <div className="flex flex-wrap gap-4 text-xs text-slate-400">
            <span>Operator: <span className="text-slate-300">{scheme.operator}</span></span>
            <span>BD Score: <span className="text-slate-300">{scheme.bd_score !== null ? Math.round(scheme.bd_score) : '--'}</span></span>
            <span>Type: <span className="text-slate-300">{scheme.scheme_type}</span></span>
          </div>
        </div>

        {/* Priority */}
        <div>
          <label className="block text-xs font-medium text-slate-400 mb-1.5">Priority</label>
          <select
            value={priority}
            onChange={(e) => setPriority(e.target.value)}
            className="w-full bg-slate-700 border border-slate-600 rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:ring-2 focus:ring-blue-500/40"
          >
            <option value="high">High</option>
            <option value="medium">Medium</option>
            <option value="low">Low</option>
          </select>
        </div>

        {/* Assigned To */}
        <div>
          <label className="block text-xs font-medium text-slate-400 mb-1.5">Assigned To (optional)</label>
          <input
            type="text"
            value={assignedTo}
            onChange={(e) => setAssignedTo(e.target.value)}
            placeholder="e.g. James Richardson"
            className="w-full bg-slate-700 border border-slate-600 rounded-lg px-3 py-2 text-sm text-white placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-blue-500/40"
          />
        </div>

        {/* Notes */}
        <div>
          <label className="block text-xs font-medium text-slate-400 mb-1.5">Notes (optional)</label>
          <textarea
            value={notes}
            onChange={(e) => setNotes(e.target.value)}
            rows={3}
            placeholder="Initial observations, strategy notes..."
            className="w-full bg-slate-700 border border-slate-600 rounded-lg px-3 py-2 text-sm text-white placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-blue-500/40 resize-none"
          />
        </div>

        {error && (
          <div className="bg-red-500/10 border border-red-500/30 rounded-lg px-3 py-2 text-sm text-red-400">
            {error}
          </div>
        )}

        {/* Actions */}
        <div className="flex justify-end gap-3 pt-2">
          <button
            onClick={onClose}
            className="px-4 py-2 text-sm text-slate-400 hover:text-white transition-colors"
          >
            Cancel
          </button>
          <button
            onClick={handleSubmit}
            disabled={submitting}
            className="px-4 py-2 text-sm font-medium text-white bg-gradient-to-r from-blue-600 to-purple-600 hover:from-blue-500 hover:to-purple-500 rounded-lg transition-all disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2"
          >
            <PlusCircleIcon className="w-4 h-4" />
            {submitting ? 'Adding...' : 'Add to Pipeline'}
          </button>
        </div>
      </div>
    </Modal>
  );
}

function paramsToFilters(sp: URLSearchParams): SchemeFilters {
  const triState = (v: string | null): 'any' | 'yes' | 'no' =>
    v === 'yes' || v === 'no' ? v : 'any';
  return {
    search: sp.get('search') || '',
    scheme_type: sp.get('scheme_type') || '',
    source: sp.get('source') || '',
    region: sp.get('region') || '',
    council_id: sp.get('council_id') || '',
    has_owner: triState(sp.get('has_owner')),
    has_operator: triState(sp.get('has_operator')),
    has_rent: triState(sp.get('has_rent')),
    min_units: sp.get('min_units') || '',
    max_units: sp.get('max_units') || '',
    min_rent: sp.get('min_rent') || '',
    max_rent: sp.get('max_rent') || '',
    operator_ids: sp.getAll('operator_id').map(s => Number(s)).filter(n => !Number.isNaN(n)),
    contract_end_within_days: sp.get('contract_end_within_days') || '',
    min_arrears: sp.get('min_arrears') || '',
  };
}

function filtersToURLSearchParams(f: SchemeFilters): URLSearchParams {
  const p = new URLSearchParams();
  if (f.search) p.set('search', f.search);
  if (f.scheme_type) p.set('scheme_type', f.scheme_type);
  if (f.source) p.set('source', f.source);
  if (f.region) p.set('region', f.region);
  if (f.council_id) p.set('council_id', f.council_id);
  if (f.has_owner !== 'any') p.set('has_owner', f.has_owner);
  if (f.has_operator !== 'any') p.set('has_operator', f.has_operator);
  if (f.has_rent !== 'any') p.set('has_rent', f.has_rent);
  if (f.min_units) p.set('min_units', f.min_units);
  if (f.max_units) p.set('max_units', f.max_units);
  if (f.min_rent) p.set('min_rent', f.min_rent);
  if (f.max_rent) p.set('max_rent', f.max_rent);
  if (f.contract_end_within_days) p.set('contract_end_within_days', f.contract_end_within_days);
  if (f.min_arrears) p.set('min_arrears', f.min_arrears);
  for (const id of f.operator_ids) p.append('operator_id', String(id));
  return p;
}

function triStateToBool(v: 'any' | 'yes' | 'no'): string | null {
  if (v === 'yes') return 'true';
  if (v === 'no') return 'false';
  return null;
}

export default function SchemesPage() {
  const router = useRouter();
  const searchParams = useSearchParams();

  const [schemes, setSchemes] = useState<SchemeRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [sortBy, setSortBy] = useState('bd_score');
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc');
  // Deep-link support: ?expanded=<scheme_id> opens that scheme's expanded row.
  // Used by inbound links from /arrears, dashboard, etc. — since there is no
  // dedicated /schemes/[id] route, this is how external surfaces reference a
  // specific scheme.
  const [expandedId, setExpandedId] = useState<string | null>(
    () => searchParams?.get('expanded') || null,
  );
  const [pipelineModalScheme, setPipelineModalScheme] = useState<SchemeRow | null>(null);
  const [page, setPage] = useState(0);
  const [totalSchemes, setTotalSchemes] = useState(0);
  const PAGE_SIZE = 100;

  // Scroll to the deep-linked scheme once it renders, and if it isn't in the
  // current page's results, fetch it directly via the scheme_ids filter and
  // prepend to the list. Without this fetch, deep-link clicks from /arrears
  // do nothing because the target scheme is often sorted past the page-1 cut.
  useEffect(() => {
    const expanded = searchParams?.get('expanded');
    if (!expanded || schemes.length === 0) return;
    const present = schemes.some(s => s.id === expanded);
    if (present) {
      const el = document.getElementById(`scheme-row-${expanded}`);
      if (el) el.scrollIntoView({ behavior: 'smooth', block: 'center' });
      return;
    }
    // Not in current page — fetch by id and prepend
    api.get('/v2/schemes', { params: { scheme_ids: expanded, limit: 1 } })
      .then(res => {
        const item = (res.data?.items || [])[0];
        if (!item) return;
        const newRow: SchemeRow = {
          id: item.id,
          name: item.name || '',
          operator: item.operator || '',
          council: item.council || '',
          region: item.region || null,
          units: item.units ?? null,
          contract_end: item.contract_end || '',
          performance: item.performance ?? null,
          satisfaction: item.satisfaction ?? null,
          bd_score: item.bd_score ?? null,
          arrears_risk_score: item.arrears_risk_score ?? null,
          priority: item.priority || 'low',
          scheme_type: item.scheme_type || 'Unknown',
          address: item.address || '',
          postcode: item.postcode || '',
          owner: item.owner || '',
          asset_manager: item.asset_manager || '',
          landlord: item.landlord || '',
          contract_start: item.contract_start || '',
          occupancy_rate: item.occupancy_rate ?? null,
          revenue_per_unit: item.revenue_per_unit ?? null,
          score_breakdown: item.score_breakdown || {
            contract_proximity: 0, performance_gap: 0,
            market_opportunity: 0, relationship_strength: 0, scheme_size: 0,
          },
          locked_fields: item.locked_fields || {},
          operator_company_id: item.operator_company_id ?? null,
          pipeline_opportunity_id: item.pipeline_opportunity_id ?? null,
          min_rent_per_week: item.min_rent_per_week ?? null,
          rent_tier_count: item.rent_tier_count ?? 0,
        };
        setSchemes(prev => [newRow, ...prev.filter(s => s.id !== newRow.id)]);
        // Scroll on next paint
        setTimeout(() => {
          const el = document.getElementById(`scheme-row-${expanded}`);
          if (el) el.scrollIntoView({ behavior: 'smooth', block: 'center' });
        }, 50);
      })
      .catch(() => {/* silent — leave list as-is */});
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [schemes.length]);

  // Initialise filters from URL. Next.js useSearchParams returns
  // ReadonlyURLSearchParams; convert to URLSearchParams via toString().
  const [filters, setFiltersState] = useState<SchemeFilters>(() =>
    paramsToFilters(new URLSearchParams(searchParams?.toString() ?? ''))
  );
  const [panelOpen, setPanelOpen] = useState(false);
  const [filterOptions, setFilterOptions] = useState<SchemesFilterOptions | null>(null);
  const [operatorLabels, setOperatorLabels] = useState<Record<number, string>>({});

  const setFilters = React.useCallback((partial: Partial<SchemeFilters>) => {
    setFiltersState(prev => {
      // Cheap equality check so setting the same value is a no-op
      let changed = false;
      for (const k of Object.keys(partial) as Array<keyof SchemeFilters>) {
        const nv = partial[k];
        const pv = prev[k];
        if (Array.isArray(nv) && Array.isArray(pv)) {
          if (nv.length !== pv.length || nv.some((v, i) => v !== pv[i])) {
            changed = true; break;
          }
        } else if (nv !== pv) {
          changed = true; break;
        }
      }
      if (!changed) return prev;

      const next = { ...prev, ...partial };
      const sp = filtersToURLSearchParams(next);
      const qs = sp.toString();
      router.replace(qs ? `/schemes?${qs}` : '/schemes', { scroll: false });
      return next;
    });
    // Reset pagination in the same tick (React 18 batches these)
    setPage(0);
  }, [router]);

  // Stable, memoised handlers for the top-row controls so they don't
  // re-subscribe child effects (e.g. SearchInput's debounce) on every render.
  const handleSearchChange = React.useCallback((v: string) => {
    setFilters({ search: v });
  }, [setFilters]);
  const handleTypeChange = React.useCallback((v: string) => {
    setFilters({ scheme_type: v });
  }, [setFilters]);
  const toggleHasRent = React.useCallback(() => {
    setFiltersState(prev => {
      const newVal = prev.has_rent === 'yes' ? 'any' : 'yes';
      if (prev.has_rent === newVal) return prev;
      const next = { ...prev, has_rent: newVal as 'yes' | 'any' };
      const sp = filtersToURLSearchParams(next);
      const qs = sp.toString();
      router.replace(qs ? `/schemes?${qs}` : '/schemes', { scroll: false });
      return next;
    });
    setPage(0);
  }, [router]);
  const clearAllFilters = React.useCallback(() => {
    setFilters(DEFAULT_FILTERS);
  }, [setFilters]);

  const handleFilterByCompetitor = React.useCallback((sourceScheme: SchemeRow, c: Competitor) => {
    // Register operator name so the pill shows nicely
    setOperatorLabels(prev => ({ ...prev, [c.operator_id]: c.operator_name }));
    setFilters({
      operator_ids: [c.operator_id],
      scheme_type: sourceScheme.scheme_type || '',
      region: sourceScheme.region || '',
      // Clear search so we're not still scoped to the source scheme
      search: '',
    });
    setExpandedId(null);
    setPanelOpen(false);
    // Scroll to top so the user sees the filtered results
    if (typeof window !== 'undefined') {
      window.scrollTo({ top: 0, behavior: 'smooth' });
    }
  }, [setFilters]);

  // Lazy-load filter options when panel first opens
  useEffect(() => {
    if (panelOpen && !filterOptions) {
      getSchemesFilterOptions()
        .then(setFilterOptions)
        .catch(() => setFilterOptions({ sources: [], scheme_types: [], regions: [] }));
    }
  }, [panelOpen, filterOptions]);

  // Load council options once for the top-row council filter
  const [councilOptions, setCouncilOptions] = useState<{ value: string; label: string }[]>([]);
  useEffect(() => {
    api.get('/v2/scheme-councils')
      .then(res => {
        const opts = (res.data || []).map((c: { id: number; name: string }) => ({
          value: String(c.id),
          label: c.name,
        }));
        setCouncilOptions(opts);
      })
      .catch(() => {});
  }, []);
  const handleCouncilChange = React.useCallback((v: string) => {
    setFilters({ council_id: v });
  }, [setFilters]);

  // Backfill operator labels for any IDs in the URL that we don't have names for
  useEffect(() => {
    const missing = filters.operator_ids.filter(id => !operatorLabels[id]);
    if (missing.length === 0) return;
    // Use the autocomplete endpoint with empty query to just get top operators,
    // or skip and let user see #id. Simpler: show #id until clicked.
  }, [filters.operator_ids, operatorLabels]);

  const fetchSchemes = React.useCallback(() => {
    setLoading(true);
    const params: Record<string, string | string[]> = {
      limit: String(PAGE_SIZE),
      skip: String(page * PAGE_SIZE),
    };
    if (filters.search) params.search = filters.search;
    if (filters.scheme_type) params.scheme_type = filters.scheme_type;
    if (filters.source) params.source = filters.source;
    if (filters.region) params.region = filters.region;
    if (filters.council_id) params.council_id = filters.council_id;
    const hasOwnerBool = triStateToBool(filters.has_owner);
    if (hasOwnerBool !== null) params.has_owner = hasOwnerBool;
    const hasOperatorBool = triStateToBool(filters.has_operator);
    if (hasOperatorBool !== null) params.has_operator = hasOperatorBool;
    const hasRentBool = triStateToBool(filters.has_rent);
    if (hasRentBool !== null) params.has_rent = hasRentBool;
    if (filters.min_units) params.min_units = filters.min_units;
    if (filters.max_units) params.max_units = filters.max_units;
    if (filters.min_rent) params.min_rent_per_week = filters.min_rent;
    if (filters.max_rent) params.max_rent_per_week = filters.max_rent;
    if (filters.contract_end_within_days) params.contract_end_within_days = filters.contract_end_within_days;
    if (filters.min_arrears) params.min_arrears = filters.min_arrears;
    if (filters.operator_ids.length > 0) {
      params.operator_company_id = filters.operator_ids.map(String);
    }
    // Server-side sort
    if (['units', 'name', 'scheme_type', 'postcode', 'contract_end', 'min_rent'].includes(sortBy)) {
      params.sort_by = sortBy;
      params.sort_dir = sortDir;
    }

    api.get('/v2/schemes', { params, paramsSerializer: { indexes: null } })
      .then(res => {
        const data = res.data;
        setTotalSchemes(data?.total ?? 0);
        const items: SchemeRow[] = (Array.isArray(data) ? data : data?.items || []).map((s: any) => ({
          id: s.id || '',
          name: s.name || 'Unnamed Scheme',
          operator: s.operator || 'Unknown Operator',
          council: s.council || 'Unknown',
          units: s.units ?? null,
          contract_end: s.contract_end || '',
          performance: s.performance ?? null,
          satisfaction: s.satisfaction ?? null,
          bd_score: s.bd_score ?? null,
          arrears_risk_score: s.arrears_risk_score ?? null,
          priority: s.priority || 'low',
          scheme_type: s.scheme_type || 'Unknown',
          address: s.address || '',
          postcode: s.postcode || '',
          owner: s.owner || '',
          asset_manager: s.asset_manager || '',
          landlord: s.landlord || '',
          contract_start: s.contract_start || '',
          occupancy_rate: s.occupancy_rate ?? null,
          revenue_per_unit: s.revenue_per_unit ?? null,
          score_breakdown: s.score_breakdown ?? {
            contract_proximity: 0, performance_gap: 0, market_opportunity: 0,
            relationship_strength: 0, scheme_size: 0,
          },
          operator_company_id: s.operator_company_id ?? null,
          pipeline_opportunity_id: s.pipeline_opportunity_id ?? null,
          locked_fields: s.locked_fields ?? {},
          min_rent_per_week: s.min_rent_per_week ?? null,
          rent_tier_count: s.rent_tier_count ?? 0,
          region: s.region ?? null,
        }));
        setSchemes(items);
        setLoading(false);
      })
      .catch(() => setLoading(false));
  }, [filters, page, sortBy, sortDir]);

  useEffect(() => {
    fetchSchemes();
  }, [fetchSchemes]);
  // Note: page is reset inside setFilters / setFiltersState callbacks, not a
  // separate effect, to avoid a second render + duplicate fetch per filter change.

  const handleSort = (field: string) => {
    if (sortBy === field) {
      setSortDir(prev => prev === 'asc' ? 'desc' : 'asc');
    } else {
      setSortBy(field);
      setSortDir(field === 'units' || field === 'bd_score' || field === 'arrears_risk_score' ? 'desc' : 'asc');
    }
  };

  const sortedSchemes = [...schemes]
    .sort((a, b) => {
      // Client-side sort for bd_score, performance, arrears (not server-sorted)
      if (sortBy === 'bd_score') {
        const diff = (b.bd_score ?? -1) - (a.bd_score ?? -1);
        return sortDir === 'asc' ? -diff : diff;
      }
      if (sortBy === 'arrears_risk_score') {
        const diff = (b.arrears_risk_score ?? -1) - (a.arrears_risk_score ?? -1);
        return sortDir === 'asc' ? -diff : diff;
      }
      if (sortBy === 'performance') {
        const diff = (a.performance ?? -1) - (b.performance ?? -1);
        return sortDir === 'asc' ? diff : -diff;
      }
      // Server-sorted fields (units, name, scheme_type, postcode, contract_end): return as-is
      return 0;
    });

  const totalSchemesCount = totalSchemes;
  const totalUnits = schemes.reduce((sum, s) => sum + (s.units ?? 0), 0);
  const schemesWithBd = schemes.filter((s) => s.bd_score !== null);
  const avgBdScore = schemesWithBd.length > 0 ? Math.round(schemesWithBd.reduce((sum, s) => sum + (s.bd_score ?? 0), 0) / schemesWithBd.length) : 0;
  const contractsAtRisk = schemes.filter((s) => {
    if (!s.contract_end) return false;
    const days = getDaysRemaining(s.contract_end);
    return days !== null && days > 0 && days < 180;
  }).length;

  const totalPages = Math.ceil(totalSchemes / PAGE_SIZE);

  // Scheme type options: prefer filter-options endpoint (full catalog); fall back
  // to the types present in the currently loaded page.
  const schemeTypeOptions = filterOptions?.scheme_types?.length
    ? filterOptions.scheme_types.map((t) => ({ value: t.value, label: t.label || t.value }))
    : Array.from(new Set(schemes.map((s) => s.scheme_type).filter(Boolean)))
        .sort()
        .map((t) => ({ value: t, label: t }));

  // Risk Matrix data
  const riskMatrixSchemes = schemes.map((s) => {
    const days = s.contract_end ? (getDaysRemaining(s.contract_end) ?? 9999) : 9999;
    const urgency = Math.max(0, Math.min(100, 100 - (days / 730) * 100));
    return { ...s, urgency };
  });

  if (loading && schemes.length === 0) {
    return (
      <div className="space-y-6">
        <div>
          <h1 className="text-2xl font-bold bg-gradient-to-r from-blue-400 via-violet-400 to-purple-400 bg-clip-text text-transparent">
            Scheme Intelligence
          </h1>
          <p className="text-sm text-slate-400 mt-1">Loading schemes...</p>
        </div>
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
          {[1, 2, 3, 4].map((i) => (
            <div key={i} className="bg-slate-800 border border-slate-700 rounded-xl p-5 animate-pulse">
              <div className="h-3 bg-slate-700 rounded w-24 mb-3" />
              <div className="h-8 bg-slate-700 rounded w-16" />
            </div>
          ))}
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-bold bg-gradient-to-r from-blue-400 via-violet-400 to-purple-400 bg-clip-text text-transparent">
          Scheme Intelligence
        </h1>
        <p className="text-sm text-slate-400 mt-1">Existing schemes with BD scoring, contract tracking, and competitive analysis</p>
      </div>

      {/* Summary stats */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        <div className="bg-slate-800 border border-slate-700 rounded-xl p-5 shadow-lg border-l-4 border-l-blue-500">
          <div className="flex items-start justify-between">
            <div>
              <p className="text-xs font-medium text-slate-400 uppercase tracking-wider">Total Schemes</p>
              <p className="mt-2 text-3xl font-bold text-white">{totalSchemesCount}</p>
            </div>
            <div className="p-2.5 bg-blue-500/10 rounded-lg">
              <BuildingOffice2Icon className="w-5 h-5 text-blue-400" />
            </div>
          </div>
        </div>

        <div className="bg-slate-800 border border-slate-700 rounded-xl p-5 shadow-lg border-l-4 border-l-emerald-500">
          <div className="flex items-start justify-between">
            <div>
              <p className="text-xs font-medium text-slate-400 uppercase tracking-wider">Total Units Managed</p>
              <p className="mt-2 text-3xl font-bold text-white">{formatNumber(totalUnits)}</p>
            </div>
            <div className="p-2.5 bg-emerald-500/10 rounded-lg">
              <HomeModernIcon className="w-5 h-5 text-emerald-400" />
            </div>
          </div>
        </div>

        <div className="bg-slate-800 border border-slate-700 rounded-xl p-5 shadow-lg border-l-4 border-l-violet-500">
          <div className="flex items-start justify-between">
            <div>
              <p className="text-xs font-medium text-slate-400 uppercase tracking-wider">Avg BD Score</p>
              <p className="mt-2 text-3xl font-bold text-white">{avgBdScore}</p>
            </div>
            <div className="p-2.5 bg-violet-500/10 rounded-lg">
              <ChartBarSquareIcon className="w-5 h-5 text-violet-400" />
            </div>
          </div>
        </div>

        <div className="bg-slate-800 border border-slate-700 rounded-xl p-5 shadow-lg border-l-4 border-l-red-500">
          <div className="flex items-start justify-between">
            <div>
              <p className="text-xs font-medium text-slate-400 uppercase tracking-wider">Contracts at Risk (&lt;6m)</p>
              <p className="mt-2 text-3xl font-bold text-white">{contractsAtRisk}</p>
            </div>
            <div className="p-2.5 bg-red-500/10 rounded-lg relative">
              <ExclamationTriangleIcon className="w-5 h-5 text-red-400" />
              <span className="absolute -top-0.5 -right-0.5 w-2.5 h-2.5 bg-red-500 rounded-full animate-pulse" />
            </div>
          </div>
        </div>
      </div>

      {/* Risk Matrix */}
      <Card>
        <h3 className="text-sm font-semibold text-white mb-4 flex items-center gap-2">
          <span className="w-1 h-4 bg-amber-500 rounded-full" />
          Opportunity Risk Matrix
        </h3>
        <div className="relative bg-slate-900/50 rounded-lg border border-slate-700/50 overflow-hidden" style={{ height: 280 }}>
          {/* Quadrant labels */}
          <div className="absolute top-2 left-2 text-[10px] text-slate-600 font-medium">Monitor</div>
          <div className="absolute top-2 right-2 text-[10px] text-red-400/80 font-bold uppercase tracking-wider">Immediate Target</div>
          <div className="absolute bottom-2 left-2 text-[10px] text-slate-600 font-medium">Low Priority</div>
          <div className="absolute bottom-2 right-2 text-[10px] text-slate-600 font-medium">Long-term Target</div>

          {/* Axis labels */}
          <div className="absolute bottom-1 left-1/2 -translate-x-1/2 text-[9px] text-slate-500 uppercase tracking-wider">
            Contract Urgency →
          </div>
          <div className="absolute left-1 top-1/2 -translate-y-1/2 -rotate-90 text-[9px] text-slate-500 uppercase tracking-wider whitespace-nowrap">
            Performance Gap →
          </div>

          {/* Quadrant dividers */}
          <div className="absolute left-1/2 top-0 bottom-0 w-px bg-slate-700/60" />
          <div className="absolute top-1/2 left-0 right-0 h-px bg-slate-700/60" />

          {/* Quadrant backgrounds */}
          <div className="absolute top-0 right-0 w-1/2 h-1/2 bg-red-500/[0.04]" />

          {/* Scheme dots */}
          {riskMatrixSchemes.map((s) => {
            const perfGap = 100 - (s.performance ?? 50); // higher gap = worse performance = higher opportunity
            const x = Math.max(8, Math.min(92, s.urgency));
            const y = Math.max(8, Math.min(92, 100 - perfGap));
            const bdScore = s.bd_score ?? 0;
            const dotColor = bdScore > 80 ? 'bg-red-500' : bdScore > 50 ? 'bg-amber-500' : 'bg-emerald-500';
            const units = s.units ?? 0;
            const dotSize = units > 500 ? 'w-4 h-4' : units > 200 ? 'w-3 h-3' : 'w-2.5 h-2.5';
            const daysLeft = s.contract_end ? getDaysRemaining(s.contract_end) : null;

            return (
              <div
                key={s.id}
                className="absolute group"
                style={{ left: `${x}%`, top: `${y}%`, transform: 'translate(-50%, -50%)' }}
              >
                <div className={cn('rounded-full shadow-lg cursor-pointer transition-transform hover:scale-150', dotColor, dotSize)} />
                <div className="hidden group-hover:block absolute z-20 bottom-full left-1/2 -translate-x-1/2 mb-2 bg-slate-800 border border-slate-600 rounded-lg px-3 py-2 shadow-xl whitespace-nowrap">
                  <p className="text-xs font-semibold text-white">{s.name}</p>
                  <p className="text-[10px] text-slate-400">
                    BD: {s.bd_score !== null ? Math.round(bdScore) : '--'} | Perf: {s.performance !== null ? `${s.performance}%` : '--'} | {daysLeft !== null ? `${daysLeft}d remaining` : 'No end date'}
                  </p>
                </div>
              </div>
            );
          })}
        </div>
      </Card>

      {/* Filters */}
      <div className="space-y-3">
        <div className="flex flex-wrap items-center gap-3">
          <SearchInput
            placeholder="Search by name, operator, owner, postcode, council, address..."
            onChange={handleSearchChange}
            className="w-80"
          />
          <Select
            options={schemeTypeOptions}
            value={filters.scheme_type}
            onChange={handleTypeChange}
            placeholder="All Types"
            className="w-48"
          />
          <Select
            options={councilOptions}
            value={filters.council_id}
            onChange={handleCouncilChange}
            placeholder="All Councils"
            className="w-56"
          />
          <button
            onClick={toggleHasRent}
            className={cn(
              'inline-flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium rounded-lg border transition-colors',
              filters.has_rent === 'yes'
                ? 'bg-amber-500/15 border-amber-500/40 text-amber-300'
                : 'bg-slate-800 border-slate-700 text-slate-400 hover:text-white hover:border-slate-600'
            )}
            title="Only show schemes with rent data"
          >
            £ Has rent data
          </button>
          <button
            onClick={() => setFilters({ min_arrears: filters.min_arrears === '60' ? '' : '60' })}
            className={cn(
              'inline-flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium rounded-lg border transition-colors',
              filters.min_arrears
                ? 'bg-orange-500/15 border-orange-500/40 text-orange-300 shadow-[0_0_0_1px_rgba(249,115,22,0.2)]'
                : 'bg-slate-800 border-slate-700 text-slate-400 hover:text-white hover:border-slate-600'
            )}
            title="Schemes whose operator shows Companies House distress signals (arrears_risk_score ≥ 60)"
          >
            <span className={cn('w-2 h-2 rounded-full', filters.min_arrears ? 'bg-orange-400' : 'bg-slate-500')} />
            Distressed only
          </button>
          <button
            onClick={() => setPanelOpen(v => !v)}
            className={cn(
              'inline-flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium rounded-lg border transition-colors',
              panelOpen || countActiveFilters(filters) > 0
                ? 'bg-blue-500/15 border-blue-500/40 text-blue-300'
                : 'bg-slate-800 border-slate-700 text-slate-400 hover:text-white hover:border-slate-600'
            )}
          >
            <AdjustmentsHorizontalIcon className="w-4 h-4" />
            More filters
            {countActiveFilters(filters) > 0 && (
              <span className="ml-0.5 inline-flex items-center justify-center min-w-[1.25rem] h-5 px-1 text-[10px] font-semibold bg-blue-500 text-white rounded-full">
                {countActiveFilters(filters)}
              </span>
            )}
          </button>
          {countActiveFilters(filters) > 0 && (
            <button
              onClick={clearAllFilters}
              className="text-xs text-slate-400 hover:text-white px-2 py-1"
            >
              Clear all
            </button>
          )}
          <span className="text-xs text-slate-500 ml-auto">
            Showing {schemes.length} of {totalSchemes.toLocaleString()} schemes
            {totalPages > 1 && ` (page ${page + 1} of ${totalPages})`}
          </span>
        </div>

        <ActiveFilterPills
          filters={filters}
          setFilters={setFilters}
          operatorLabels={operatorLabels}
        />

        {panelOpen && (
          <FilterPanel
            filters={filters}
            setFilters={setFilters}
            filterOptions={filterOptions}
            operatorLabels={operatorLabels}
            setOperatorLabels={setOperatorLabels}
          />
        )}
      </div>

      {/* Table */}
      <Card noPadding>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-slate-700">
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase w-8"></th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase cursor-pointer hover:text-white transition-colors select-none" onClick={() => handleSort('name')}>
                  Name {sortBy === 'name' && <span className="text-blue-400">{sortDir === 'asc' ? ' \u25B2' : ' \u25BC'}</span>}
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase">Operator</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase cursor-pointer hover:text-white transition-colors select-none" onClick={() => handleSort('scheme_type')}>
                  Type {sortBy === 'scheme_type' && <span className="text-blue-400">{sortDir === 'asc' ? ' \u25B2' : ' \u25BC'}</span>}
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase cursor-pointer hover:text-white transition-colors select-none" onClick={() => handleSort('units')}>
                  Units {sortBy === 'units' && <span className="text-blue-400">{sortDir === 'asc' ? ' \u25B2' : ' \u25BC'}</span>}
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase cursor-pointer hover:text-white transition-colors select-none" onClick={() => handleSort('contract_end')}>
                  Contract End {sortBy === 'contract_end' && <span className="text-blue-400">{sortDir === 'asc' ? ' \u25B2' : ' \u25BC'}</span>}
                </th>
                <th className="px-4 py-3 text-right text-xs font-medium text-slate-400 uppercase cursor-pointer hover:text-white transition-colors select-none" onClick={() => handleSort('min_rent')}>
                  From £/wk {sortBy === 'min_rent' && <span className="text-amber-400">{sortDir === 'asc' ? ' \u25B2' : ' \u25BC'}</span>}
                </th>
                <th className="px-4 py-3 text-center text-xs font-medium text-slate-400 uppercase cursor-pointer hover:text-white transition-colors select-none" onClick={() => handleSort('performance')}>
                  Perf. {sortBy === 'performance' && <span className="text-blue-400">{sortDir === 'asc' ? ' \u25B2' : ' \u25BC'}</span>}
                </th>
                <th className="px-4 py-3 text-center text-xs font-medium text-slate-400 uppercase">Satisf.</th>
                <th className="px-4 py-3 text-center text-xs font-medium text-slate-400 uppercase cursor-pointer hover:text-white transition-colors select-none" onClick={() => handleSort('bd_score')}>
                  BD Score {sortBy === 'bd_score' && <span className="text-blue-400">{sortDir === 'asc' ? ' \u25B2' : ' \u25BC'}</span>}
                </th>
                <th
                  className="px-4 py-3 text-center text-xs font-medium text-slate-400 uppercase cursor-pointer hover:text-white transition-colors select-none"
                  onClick={() => handleSort('arrears_risk_score')}
                  title="Operator financial distress score (Companies House). Higher = more distress = stronger BD opportunity."
                >
                  Arrears {sortBy === 'arrears_risk_score' && <span className="text-orange-400">{sortDir === 'asc' ? ' \u25B2' : ' \u25BC'}</span>}
                </th>
                <th className="px-4 py-3 text-center text-xs font-medium text-slate-400 uppercase">Trend</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase">Priority</th>
                <th className="px-4 py-3 text-center text-xs font-medium text-slate-400 uppercase">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-700/50">
              {sortedSchemes.map((scheme) => {
                const daysRemaining = scheme.contract_end ? getDaysRemaining(scheme.contract_end) : null;
                const trend = 0;

                return (
                  <React.Fragment key={scheme.id}>
                    <tr
                      id={`scheme-row-${scheme.id}`}
                      className={cn(
                        'hover:bg-slate-700/50 transition-colors cursor-pointer',
                        (scheme.bd_score ?? 0) >= 85 && 'bg-gradient-to-r from-red-500/[0.04] via-transparent to-transparent'
                      )}
                      onClick={() => setExpandedId(expandedId === scheme.id ? null : scheme.id)}
                    >
                      <td className="px-4 py-3">
                        {expandedId === scheme.id
                          ? <ChevronUpIcon className="w-4 h-4 text-slate-400" />
                          : <ChevronDownIcon className="w-4 h-4 text-slate-400" />
                        }
                      </td>
                      <td className="px-4 py-3">
                        <div>
                          <span className="font-medium text-white">{scheme.name}</span>
                          <p className="text-[10px] text-slate-500 mt-0.5">{scheme.council}</p>
                        </div>
                      </td>
                      <td className="px-4 py-3 text-slate-400 text-xs">{scheme.operator}</td>
                      <td className="px-4 py-3"><Badge variant={getSchemeTypeColor(scheme.scheme_type)}>{scheme.scheme_type}</Badge></td>
                      <td className="px-4 py-3 text-slate-300 font-medium">{scheme.units ?? <span className="text-slate-500">--</span>}</td>
                      <td className="px-4 py-3">
                        {scheme.contract_end ? (
                          <div>
                            <span className={cn('text-xs font-medium block', getContractEndColor(scheme.contract_end))}>
                              {formatDate(scheme.contract_end)}
                            </span>
                            <span className={cn('text-[10px] font-medium', getDaysRemainingColor(daysRemaining ?? 9999))}>
                              {daysRemaining !== null && daysRemaining > 0 ? `${daysRemaining} days remaining` : daysRemaining !== null ? 'Expired' : ''}
                            </span>
                          </div>
                        ) : (
                          <span className="text-xs text-slate-500">Not set</span>
                        )}
                      </td>
                      <td className="px-4 py-3 text-right">
                        {scheme.min_rent_per_week !== null ? (
                          <div>
                            <span className="text-sm font-semibold text-amber-300">£{scheme.min_rent_per_week.toFixed(0)}</span>
                            <span className="text-[10px] text-slate-500 ml-1">/wk</span>
                            {scheme.rent_tier_count > 1 && (
                              <div className="text-[10px] text-slate-500">{scheme.rent_tier_count} tiers</div>
                            )}
                          </div>
                        ) : (
                          <span className="text-xs text-slate-600">--</span>
                        )}
                      </td>
                      <td className="px-4 py-3 text-center">
                        <GaugeRing value={scheme.performance} color={getGaugeColor(scheme.performance)} />
                      </td>
                      <td className="px-4 py-3 text-center">
                        <GaugeRing value={scheme.satisfaction} color={getGaugeColor(scheme.satisfaction)} />
                      </td>
                      <td className="px-4 py-3 text-center">
                        <BdScoreRadial score={scheme.bd_score} />
                      </td>
                      <td className="px-4 py-3 text-center">
                        {scheme.arrears_risk_score !== null ? (
                          (() => {
                            const s = scheme.arrears_risk_score!;
                            const cls = s >= 80
                              ? 'bg-red-500/20 text-red-300 border-red-500/40'
                              : s >= 60
                                ? 'bg-orange-500/20 text-orange-300 border-orange-500/40'
                                : s >= 35
                                  ? 'bg-amber-500/15 text-amber-300 border-amber-500/30'
                                  : 'bg-slate-700/50 text-slate-400 border-slate-600';
                            return (
                              <span
                                className={cn(
                                  'inline-flex items-center justify-center min-w-[2.25rem] px-1.5 py-0.5 rounded text-[11px] font-semibold border tabular-nums',
                                  cls,
                                )}
                                title={`Arrears risk ${s.toFixed(0)} — derived from operator's Companies House signals`}
                              >
                                {s.toFixed(0)}
                              </span>
                            );
                          })()
                        ) : (
                          <span className="text-[10px] text-slate-600">--</span>
                        )}
                      </td>
                      <td className="px-4 py-3 text-center">
                        {trend > 0 ? (
                          <div className="flex items-center justify-center gap-0.5">
                            <ArrowTrendingUpIcon className="w-4 h-4 text-red-400" />
                            <span className="text-[10px] font-semibold text-red-400">+{trend}</span>
                          </div>
                        ) : trend < 0 ? (
                          <div className="flex items-center justify-center gap-0.5">
                            <ArrowTrendingDownIcon className="w-4 h-4 text-emerald-400" />
                            <span className="text-[10px] font-semibold text-emerald-400">{trend}</span>
                          </div>
                        ) : (
                          <span className="text-[10px] text-slate-600">--</span>
                        )}
                      </td>
                      <td className="px-4 py-3">
                        <div className="flex items-center gap-1.5">
                          <div className={cn(
                            'w-2.5 h-2.5 rounded-full',
                            scheme.priority === 'high' ? 'bg-red-500' :
                            scheme.priority === 'medium' ? 'bg-amber-500' : 'bg-emerald-500'
                          )} />
                          <span className="text-xs text-slate-400 capitalize">{scheme.priority}</span>
                        </div>
                      </td>
                      <td className="px-4 py-3 text-center">
                        {scheme.pipeline_opportunity_id ? (
                          <a
                            href="/pipeline"
                            onClick={(e) => e.stopPropagation()}
                            className="inline-flex items-center gap-1 px-2.5 py-1 text-[11px] font-medium text-emerald-400 bg-emerald-500/10 border border-emerald-500/30 rounded-full hover:bg-emerald-500/20 transition-colors"
                          >
                            <CheckCircleIcon className="w-3.5 h-3.5" />
                            In Pipeline
                          </a>
                        ) : scheme.operator_company_id ? (
                          <PermissionGate resource="pipeline" action="create" fallback={<span className="text-[11px] text-slate-600">--</span>}>
                            <button
                              onClick={(e) => { e.stopPropagation(); setPipelineModalScheme(scheme); }}
                              className="inline-flex items-center gap-1 px-2.5 py-1 text-[11px] font-medium text-blue-400 bg-blue-500/10 border border-blue-500/30 rounded-full hover:bg-blue-500/20 transition-colors"
                            >
                              <PlusCircleIcon className="w-3.5 h-3.5" />
                              Add to Pipeline
                            </button>
                          </PermissionGate>
                        ) : (
                          <span className="text-[11px] text-slate-600">No operator</span>
                        )}
                      </td>
                    </tr>

                    {/* Expanded row */}
                    {expandedId === scheme.id && (
                      <tr>
                        <td colSpan={14} className="px-8 py-6 bg-slate-800/60">
                          <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                            {/* Scheme Details */}
                            <div className="space-y-4">
                              <h4 className="text-sm font-semibold text-white flex items-center gap-2">
                                <span className="w-1 h-4 bg-blue-500 rounded-full" />
                                Scheme Details
                              </h4>
                              <div className="space-y-2 text-sm">
                                <div className="flex justify-between items-center gap-2">
                                  <span className="text-slate-500">Address</span>
                                  <InlineFieldEdit schemeId={scheme.id} field="address" value={scheme.address}
                                    lockedBy={scheme.locked_fields?.address} onSaved={() => fetchSchemes()} />
                                </div>
                                <div className="flex justify-between items-center gap-2">
                                  <span className="text-slate-500">Postcode</span>
                                  <InlineFieldEdit schemeId={scheme.id} field="postcode" value={scheme.postcode}
                                    lockedBy={scheme.locked_fields?.postcode} onSaved={() => fetchSchemes()} />
                                </div>
                                <div className="flex justify-between items-center gap-2">
                                  <span className="text-slate-500">Num Units</span>
                                  <InlineFieldEdit schemeId={scheme.id} field="num_units" value={scheme.units} type="number"
                                    lockedBy={scheme.locked_fields?.num_units} onSaved={() => fetchSchemes()} />
                                </div>
                                <div className="flex justify-between items-center gap-2">
                                  <span className="text-slate-500">Operator</span>
                                  <InlineFieldEdit schemeId={scheme.id} field="operator" value={scheme.operator} type="company"
                                    lockedBy={scheme.locked_fields?.operator_company_id} onSaved={() => fetchSchemes()} />
                                </div>
                                <div className="flex justify-between items-center gap-2">
                                  <span className="text-slate-500">Owner</span>
                                  <InlineFieldEdit schemeId={scheme.id} field="owner" value={scheme.owner} type="company"
                                    lockedBy={scheme.locked_fields?.owner_company_id} onSaved={() => fetchSchemes()} />
                                </div>
                                <div className="flex justify-between items-center gap-2">
                                  <span className="text-slate-500">Asset Manager</span>
                                  <InlineFieldEdit schemeId={scheme.id} field="asset_manager" value={scheme.asset_manager} type="company"
                                    lockedBy={scheme.locked_fields?.asset_manager_company_id} onSaved={() => fetchSchemes()} />
                                </div>
                                <div className="flex justify-between items-center gap-2">
                                  <span className="text-slate-500">Landlord</span>
                                  <InlineFieldEdit schemeId={scheme.id} field="landlord" value={scheme.landlord} type="company"
                                    lockedBy={scheme.locked_fields?.landlord_company_id} onSaved={() => fetchSchemes()} />
                                </div>
                                <div className="flex justify-between items-center gap-2">
                                  <span className="text-slate-500">Contract Start</span>
                                  <InlineFieldEdit schemeId={scheme.id} field="contract_start_date" value={scheme.contract_start} type="date"
                                    lockedBy={scheme.locked_fields?.contract_start_date} onSaved={() => fetchSchemes()}
                                    displayFormatter={(v) => v ? formatDate(String(v)) : <span className="text-slate-600">--</span>} />
                                </div>
                                <div className="flex justify-between items-center gap-2">
                                  <span className="text-slate-500">Contract End</span>
                                  <InlineFieldEdit schemeId={scheme.id} field="contract_end_date" value={scheme.contract_end} type="date"
                                    lockedBy={scheme.locked_fields?.contract_end_date} onSaved={() => fetchSchemes()}
                                    displayFormatter={(v) => v ? <span className={cn('font-medium', getContractEndColor(String(v)))}>{formatDate(String(v))}</span> : <span className="text-slate-600">Not set</span>} />
                                </div>
                              </div>
                            </div>

                            {/* BD Score Breakdown */}
                            <BDScoreBreakdown scheme={scheme} />

                            {/* Competitor Analysis */}
                            <CompetitorPanel scheme={scheme} onFilterBy={(c) => handleFilterByCompetitor(scheme, c)} />
                          </div>

                          {/* Operator Health (arrears / Companies House distress signals) */}
                          <OperatorHealthPanel scheme={scheme} />

                          {/* Ownership chain (PSC walk: owner SPV → platform → ultimate owner) */}
                          <OwnershipPanel schemeId={scheme.id} />

                          {/* Rent Tiers */}
                          <RentPanel schemeId={scheme.id} />

                          {/* Contract History */}
                          <ContractTimeline schemeId={scheme.id} />

                          {/* AI Enrichment + Actions */}
                          <div className="mt-6 flex items-start gap-6">
                            <div className="flex-1">
                              <AIEnrichPanel
                                schemeId={scheme.id}
                                schemeName={scheme.name}
                                onApplied={() => fetchSchemes()}
                              />
                            </div>
                            <div className="flex-shrink-0 pt-2">
                              {scheme.pipeline_opportunity_id ? (
                                <a
                                  href="/pipeline"
                                  className="inline-flex items-center gap-2 px-4 py-2.5 text-sm font-medium text-emerald-400 bg-emerald-500/10 border border-emerald-500/30 rounded-lg hover:bg-emerald-500/20 transition-colors"
                                >
                                  <CheckCircleIcon className="w-5 h-5" />
                                  View in Pipeline
                                </a>
                              ) : scheme.operator_company_id ? (
                                <PermissionGate resource="pipeline" action="create">
                                  <button
                                    onClick={(e) => { e.stopPropagation(); setPipelineModalScheme(scheme); }}
                                    className="inline-flex items-center gap-2 px-4 py-2.5 text-sm font-medium text-white bg-gradient-to-r from-blue-600 to-purple-600 hover:from-blue-500 hover:to-purple-500 rounded-lg transition-all shadow-lg shadow-blue-500/20"
                                  >
                                    <PlusCircleIcon className="w-5 h-5" />
                                    Add to Pipeline
                                  </button>
                                </PermissionGate>
                              ) : null}
                            </div>
                          </div>
                        </td>
                      </tr>
                    )}
                  </React.Fragment>
                );
              })}
            </tbody>
          </table>
        </div>
      </Card>

      {/* Pagination Controls */}
      {totalPages > 1 && (
        <div className="flex items-center justify-between">
          <span className="text-xs text-slate-500">
            Showing {page * PAGE_SIZE + 1}–{Math.min((page + 1) * PAGE_SIZE, totalSchemes)} of {totalSchemes.toLocaleString()} schemes
          </span>
          <div className="flex items-center gap-2">
            <button
              onClick={() => setPage(0)}
              disabled={page === 0}
              className="px-3 py-1.5 text-xs font-medium text-slate-400 bg-slate-800 border border-slate-700 rounded-lg hover:bg-slate-700 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
            >
              First
            </button>
            <button
              onClick={() => setPage((p) => Math.max(0, p - 1))}
              disabled={page === 0}
              className="px-3 py-1.5 text-xs font-medium text-slate-400 bg-slate-800 border border-slate-700 rounded-lg hover:bg-slate-700 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
            >
              ← Prev
            </button>
            <div className="flex items-center gap-1">
              {Array.from({ length: Math.min(7, totalPages) }, (_, i) => {
                let pageNum: number;
                if (totalPages <= 7) {
                  pageNum = i;
                } else if (page < 4) {
                  pageNum = i;
                } else if (page > totalPages - 5) {
                  pageNum = totalPages - 7 + i;
                } else {
                  pageNum = page - 3 + i;
                }
                return (
                  <button
                    key={pageNum}
                    onClick={() => setPage(pageNum)}
                    className={cn(
                      'w-8 h-8 text-xs font-medium rounded-lg transition-colors',
                      pageNum === page
                        ? 'bg-blue-600 text-white'
                        : 'text-slate-400 bg-slate-800 border border-slate-700 hover:bg-slate-700'
                    )}
                  >
                    {pageNum + 1}
                  </button>
                );
              })}
            </div>
            <button
              onClick={() => setPage((p) => Math.min(totalPages - 1, p + 1))}
              disabled={page >= totalPages - 1}
              className="px-3 py-1.5 text-xs font-medium text-slate-400 bg-slate-800 border border-slate-700 rounded-lg hover:bg-slate-700 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
            >
              Next →
            </button>
            <button
              onClick={() => setPage(totalPages - 1)}
              disabled={page >= totalPages - 1}
              className="px-3 py-1.5 text-xs font-medium text-slate-400 bg-slate-800 border border-slate-700 rounded-lg hover:bg-slate-700 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
            >
              Last
            </button>
          </div>
        </div>
      )}

      {/* Add to Pipeline Modal */}
      <AddToPipelineModal
        scheme={pipelineModalScheme}
        isOpen={pipelineModalScheme !== null}
        onClose={() => setPipelineModalScheme(null)}
        onSuccess={(schemeId, opportunityId) => {
          setSchemes((prev) =>
            prev.map((s) =>
              s.id === schemeId ? { ...s, pipeline_opportunity_id: opportunityId } : s
            )
          );
        }}
      />
    </div>
  );
}
