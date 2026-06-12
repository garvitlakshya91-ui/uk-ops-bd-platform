'use client';

import React, { useState, useEffect, useMemo } from 'react';
import Link from 'next/link';
import {
  ExclamationTriangleIcon,
  FireIcon,
  ArrowTrendingUpIcon,
  BuildingOffice2Icon,
  ArrowDownTrayIcon,
  ChartBarIcon,
  ClockIcon,
} from '@heroicons/react/24/outline';
import api from '@/lib/api';
import { cn } from '@/lib/utils';
import Card from '@/components/ui/Card';
import Badge from '@/components/ui/Badge';

// ---------------------------------------------------------------------------
// Types matching the /api/v2/arrears/overview response
// ---------------------------------------------------------------------------

interface ArrearsKPIs {
  critical_count: number;
  distressed_count: number;
  caution_count: number;
  healthy_count: number;
  distressed_operators: number;
  newly_flagged_7d: number;
}

interface PlayCard {
  key: string;
  title: string;
  trigger: string;
  count: number;
  top: Array<Record<string, unknown>>;
}

interface OperatorRow {
  company_id: number;
  company_name: string;
  ch_number?: string | null;
  scheme_count: number;
  avg_arrears: number;
  max_arrears: number;
  critical_count: number;
  latest_signal?: string | null;
  sample_schemes: Array<{ scheme_id: number; name: string; arrears_score: number; units?: number | null }>;
  last_checked?: string | null;
}

interface SchemeRow {
  scheme_id: number;
  name: string;
  postcode?: string | null;
  council?: string | null;
  operator?: string | null;
  operator_company_id?: number | null;
  scheme_type?: string | null;
  units?: number | null;
  arrears_score: number;
  bucket: 'critical' | 'distressed' | 'caution' | 'healthy';
  top_signal?: string | null;
  contract_end_date?: string | null;
  bd_score?: number | null;
  last_checked?: string | null;
}

interface Signal {
  scheme_id: number;
  scheme_name: string;
  operator?: string | null;
  arrears_score: number;
  last_checked: string;
  summary: string;
}

interface OverviewResponse {
  kpis: ArrearsKPIs;
  plays: PlayCard[];
  top_operators: OperatorRow[];
  hot_schemes: SchemeRow[];
  recent_signals: Signal[];
  distribution: { healthy: number; caution: number; distressed: number; critical: number };
  generated_at: string;
  total_scored: number;
  total_bd_cohort: number;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function bucketBadge(bucket: string, score: number) {
  if (bucket === 'critical')
    return (
      <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-md text-xs font-semibold bg-red-500/20 text-red-300 border border-red-500/40">
        <FireIcon className="w-3 h-3" /> {score.toFixed(0)}
      </span>
    );
  if (bucket === 'distressed')
    return (
      <span className="inline-flex items-center px-2 py-0.5 rounded-md text-xs font-semibold bg-orange-500/20 text-orange-300 border border-orange-500/40">
        {score.toFixed(0)}
      </span>
    );
  if (bucket === 'caution')
    return (
      <span className="inline-flex items-center px-2 py-0.5 rounded-md text-xs font-semibold bg-amber-500/15 text-amber-300 border border-amber-500/30">
        {score.toFixed(0)}
      </span>
    );
  return (
    <span className="inline-flex items-center px-2 py-0.5 rounded-md text-xs font-semibold bg-slate-700/50 text-slate-400 border border-slate-600">
      {score.toFixed(0)}
    </span>
  );
}

function formatRelative(iso?: string | null) {
  if (!iso) return '—';
  const d = new Date(iso);
  const mins = Math.floor((Date.now() - d.getTime()) / 60000);
  if (mins < 60) return `${mins}m ago`;
  if (mins < 1440) return `${Math.floor(mins / 60)}h ago`;
  return `${Math.floor(mins / 1440)}d ago`;
}

function formatDate(iso?: string | null) {
  if (!iso) return '—';
  return new Date(iso).toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' });
}

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

export default function ArrearsPage() {
  const [data, setData] = useState<OverviewResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [hotFilter, setHotFilter] = useState<'all' | 'critical' | 'distressed' | 'caution'>('all');

  useEffect(() => {
    setLoading(true);
    api.get('/v2/arrears/overview')
      .then((res) => setData(res.data))
      .catch((err) => setError(err?.response?.data?.detail || 'Failed to load arrears data'))
      .finally(() => setLoading(false));
  }, []);

  const filteredHot = useMemo(() => {
    if (!data) return [];
    if (hotFilter === 'all') return data.hot_schemes;
    return data.hot_schemes.filter((s) => s.bucket === hotFilter);
  }, [data, hotFilter]);

  if (loading)
    return (
      <div className="p-8">
        <div className="animate-pulse space-y-4">
          <div className="h-8 w-64 bg-slate-800 rounded" />
          <div className="grid grid-cols-4 gap-3">
            {[1, 2, 3, 4].map((i) => (
              <div key={i} className="h-24 bg-slate-800 rounded-lg" />
            ))}
          </div>
        </div>
      </div>
    );

  if (error)
    return (
      <div className="p-8">
        <div className="bg-red-500/10 border border-red-500/30 rounded-lg p-6 text-red-300">{error}</div>
      </div>
    );

  if (!data) return null;

  const k = data.kpis;
  const coveragePct = data.total_bd_cohort ? (100 * data.total_scored) / data.total_bd_cohort : 0;
  const distMax = Math.max(data.distribution.healthy, data.distribution.caution, data.distribution.distressed, data.distribution.critical);

  return (
    <div className="p-6 space-y-6 max-w-[1600px] mx-auto">
      {/* Header */}
      <div className="flex items-start justify-between">
        <div>
          <h1 className="text-2xl font-semibold text-white flex items-center gap-2">
            <ExclamationTriangleIcon className="w-7 h-7 text-amber-400" />
            Arrears & Operator Distress
          </h1>
          <p className="mt-1 text-sm text-slate-400">
            Real-time financial distress signals from Companies House. Coverage:{' '}
            <span className="text-white font-medium">
              {data.total_scored.toLocaleString()} / {data.total_bd_cohort.toLocaleString()}
            </span>{' '}
            BTR/PBSA/Senior/Co-living schemes ({coveragePct.toFixed(1)}%)
          </p>
        </div>
        <div className="text-xs text-slate-500 flex items-center gap-1">
          <ClockIcon className="w-3.5 h-3.5" />
          Generated {formatRelative(data.generated_at)}
        </div>
      </div>

      {/* KPI cards */}
      <div className="grid grid-cols-4 gap-3">
        <KpiCard
          label="Critical (≥80)"
          value={k.critical_count}
          tone="red"
          icon={<FireIcon className="w-5 h-5" />}
          sub="Likely dissolved or in administration"
        />
        <KpiCard
          label="Distressed (60-79)"
          value={k.distressed_count}
          tone="orange"
          icon={<ExclamationTriangleIcon className="w-5 h-5" />}
          sub="Multiple distress flags"
        />
        <KpiCard
          label="Distressed Operators"
          value={k.distressed_operators}
          tone="amber"
          icon={<BuildingOffice2Icon className="w-5 h-5" />}
          sub="Portfolio avg ≥ 50 across ≥ 2 schemes"
        />
        <KpiCard
          label="Newly Flagged (7d)"
          value={k.newly_flagged_7d}
          tone="purple"
          icon={<ArrowTrendingUpIcon className="w-5 h-5" />}
          sub="New distress signals in last week"
        />
      </div>

      {/* BD play cards */}
      <div>
        <h2 className="text-sm font-semibold text-slate-300 uppercase tracking-wide mb-3">BD Plays Activated</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-3">
          {data.plays.map((p) => (
            <PlayCardComponent key={p.key} play={p} />
          ))}
        </div>
      </div>

      {/* Operator leaderboard + Distribution chart in two columns */}
      <div className="grid grid-cols-1 xl:grid-cols-3 gap-4">
        <div className="xl:col-span-2">
          <Card className="overflow-hidden">
            <div className="px-5 py-4 border-b border-slate-700 flex items-center justify-between">
              <div>
                <h2 className="text-sm font-semibold text-white">Operator Leaderboard — by avg arrears</h2>
                <p className="text-xs text-slate-500 mt-0.5">
                  {data.top_operators.length === 0
                    ? 'No operators meet the distress threshold yet (avg ≥ 50, ≥ 2 schemes).'
                    : `Top ${data.top_operators.length} distressed operators.`}
                </p>
              </div>
            </div>
            <div className="overflow-x-auto">
              <table className="min-w-full text-sm">
                <thead>
                  <tr className="text-xs text-slate-500 uppercase tracking-wide bg-slate-800/40">
                    <th className="text-left px-4 py-2 font-medium">Operator</th>
                    <th className="text-right px-3 py-2 font-medium">Schemes</th>
                    <th className="text-right px-3 py-2 font-medium">Avg</th>
                    <th className="text-right px-3 py-2 font-medium">Max</th>
                    <th className="text-right px-3 py-2 font-medium">Critical</th>
                    <th className="text-left px-3 py-2 font-medium">Latest signal</th>
                    <th className="text-right px-3 py-2 font-medium">Last check</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-800">
                  {data.top_operators.length === 0 && (
                    <tr>
                      <td colSpan={7} className="text-center py-6 text-slate-500">
                        No operators in distress yet. Operator-linked arrears coverage is still building.
                      </td>
                    </tr>
                  )}
                  {data.top_operators.map((op) => (
                    <tr key={op.company_id} className="hover:bg-slate-800/40 transition-colors">
                      <td className="px-4 py-2.5">
                        <Link
                          href={`/companies?expanded=${op.company_id}`}
                          className="text-white hover:text-amber-300 font-medium"
                        >
                          {op.company_name}
                        </Link>
                        {op.ch_number && (
                          <span className="ml-2 text-[10px] text-slate-500">CH {op.ch_number}</span>
                        )}
                        {op.sample_schemes.length > 0 && (
                          <div className="mt-1 text-[11px] text-slate-500 truncate max-w-md">
                            {op.sample_schemes.map((s) => s.name).join(' · ')}
                          </div>
                        )}
                      </td>
                      <td className="text-right px-3 py-2.5 tabular-nums text-slate-300">{op.scheme_count}</td>
                      <td className="text-right px-3 py-2.5 tabular-nums">
                        {bucketBadge(
                          op.avg_arrears >= 80 ? 'critical' : op.avg_arrears >= 60 ? 'distressed' : 'caution',
                          op.avg_arrears,
                        )}
                      </td>
                      <td className="text-right px-3 py-2.5 tabular-nums">
                        {bucketBadge(
                          op.max_arrears >= 80 ? 'critical' : op.max_arrears >= 60 ? 'distressed' : 'caution',
                          op.max_arrears,
                        )}
                      </td>
                      <td className="text-right px-3 py-2.5 tabular-nums">
                        {op.critical_count > 0 ? (
                          <span className="text-red-300 font-semibold">{op.critical_count}</span>
                        ) : (
                          <span className="text-slate-500">—</span>
                        )}
                      </td>
                      <td className="px-3 py-2.5 text-xs text-slate-400">{op.latest_signal || '—'}</td>
                      <td className="text-right px-3 py-2.5 text-[11px] text-slate-500">
                        {formatRelative(op.last_checked)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </Card>
        </div>

        {/* Distribution chart */}
        <div>
          <Card className="p-5">
            <h2 className="text-sm font-semibold text-white flex items-center gap-2">
              <ChartBarIcon className="w-4 h-4 text-slate-400" />
              Distribution
            </h2>
            <p className="text-xs text-slate-500 mt-0.5 mb-4">All {data.total_scored.toLocaleString()} scored schemes</p>
            <div className="space-y-2">
              {[
                { label: 'Critical (≥80)', count: data.distribution.critical, color: 'bg-red-500', text: 'text-red-300' },
                { label: 'Distressed (60-79)', count: data.distribution.distressed, color: 'bg-orange-500', text: 'text-orange-300' },
                { label: 'Caution (35-59)', count: data.distribution.caution, color: 'bg-amber-500', text: 'text-amber-300' },
                { label: 'Healthy (0-34)', count: data.distribution.healthy, color: 'bg-emerald-500/60', text: 'text-emerald-300' },
              ].map((row) => (
                <div key={row.label}>
                  <div className="flex items-center justify-between text-xs mb-1">
                    <span className="text-slate-400">{row.label}</span>
                    <span className={cn('font-semibold tabular-nums', row.text)}>{row.count.toLocaleString()}</span>
                  </div>
                  <div className="h-2.5 bg-slate-800 rounded-full overflow-hidden">
                    <div
                      className={cn('h-full rounded-full transition-all', row.color)}
                      style={{ width: `${distMax ? (100 * row.count) / distMax : 0}%` }}
                    />
                  </div>
                </div>
              ))}
            </div>
            <div className="mt-5 pt-4 border-t border-slate-800 text-xs text-slate-500">
              Score = inverse of operator's Companies House health. Higher = more distress = stronger BD opportunity.
            </div>
          </Card>
        </div>
      </div>

      {/* Scheme hot list */}
      <Card className="overflow-hidden">
        <div className="px-5 py-4 border-b border-slate-700 flex items-center justify-between flex-wrap gap-3">
          <div>
            <h2 className="text-sm font-semibold text-white">Scheme Distress Hot List</h2>
            <p className="text-xs text-slate-500 mt-0.5">Top 100 BD-cohort schemes by arrears score</p>
          </div>
          <div className="flex gap-1.5">
            {(['all', 'critical', 'distressed', 'caution'] as const).map((b) => (
              <button
                key={b}
                onClick={() => setHotFilter(b)}
                className={cn(
                  'text-xs px-3 py-1.5 rounded-md font-medium border transition-colors',
                  hotFilter === b
                    ? 'bg-amber-500/15 text-amber-300 border-amber-500/40'
                    : 'bg-slate-800 text-slate-400 border-slate-700 hover:text-white',
                )}
              >
                {b === 'all' ? 'All' : b[0].toUpperCase() + b.slice(1)}
              </button>
            ))}
            <button className="text-xs px-3 py-1.5 rounded-md font-medium bg-slate-800 text-slate-400 border border-slate-700 hover:text-white inline-flex items-center gap-1">
              <ArrowDownTrayIcon className="w-3.5 h-3.5" />
              CSV
            </button>
          </div>
        </div>
        <div className="overflow-x-auto">
          <table className="min-w-full text-sm">
            <thead>
              <tr className="text-xs text-slate-500 uppercase tracking-wide bg-slate-800/40">
                <th className="text-left px-4 py-2 font-medium">Scheme</th>
                <th className="text-left px-3 py-2 font-medium">Operator</th>
                <th className="text-left px-3 py-2 font-medium">Council</th>
                <th className="text-left px-3 py-2 font-medium">Type</th>
                <th className="text-right px-3 py-2 font-medium">Units</th>
                <th className="text-right px-3 py-2 font-medium">Arrears</th>
                <th className="text-left px-3 py-2 font-medium">Top signal</th>
                <th className="text-left px-3 py-2 font-medium">Contract ends</th>
                <th className="text-right px-3 py-2 font-medium">BD score</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800">
              {filteredHot.length === 0 && (
                <tr>
                  <td colSpan={9} className="text-center py-6 text-slate-500">
                    No schemes match this filter.
                  </td>
                </tr>
              )}
              {filteredHot.map((s) => (
                <tr key={s.scheme_id} className="hover:bg-slate-800/40 transition-colors">
                  <td className="px-4 py-2">
                    <Link
                      href={`/schemes?expanded=${s.scheme_id}`}
                      className="text-white hover:text-amber-300 font-medium"
                    >
                      {s.name}
                    </Link>
                    {s.postcode && <span className="ml-2 text-[10px] text-slate-500">{s.postcode}</span>}
                  </td>
                  <td className="px-3 py-2 text-slate-300">
                    {s.operator ? (
                      s.operator_company_id ? (
                        <Link href={`/companies?expanded=${s.operator_company_id}`} className="hover:text-amber-300">
                          {s.operator}
                        </Link>
                      ) : (
                        s.operator
                      )
                    ) : (
                      <span className="text-slate-500">—</span>
                    )}
                  </td>
                  <td className="px-3 py-2 text-slate-400">{s.council || '—'}</td>
                  <td className="px-3 py-2">
                    {s.scheme_type ? <Badge variant="default">{s.scheme_type}</Badge> : null}
                  </td>
                  <td className="text-right px-3 py-2 tabular-nums text-slate-400">{s.units ?? '—'}</td>
                  <td className="text-right px-3 py-2 tabular-nums">{bucketBadge(s.bucket, s.arrears_score)}</td>
                  <td className="px-3 py-2 text-xs text-slate-400">{s.top_signal || '—'}</td>
                  <td className="px-3 py-2 text-xs text-slate-400">{formatDate(s.contract_end_date)}</td>
                  <td className="text-right px-3 py-2 tabular-nums text-slate-300">
                    {s.bd_score?.toFixed(1) ?? '—'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Card>

      {/* Recent signals */}
      <Card className="p-5">
        <h2 className="text-sm font-semibold text-white">Recent Distress Signals — last 7 days</h2>
        <p className="text-xs text-slate-500 mt-0.5 mb-4">
          {data.recent_signals.length === 0
            ? 'No new distress flags in the last 7 days.'
            : `${data.recent_signals.length} signal${data.recent_signals.length === 1 ? '' : 's'} in the last week.`}
        </p>
        <div className="space-y-2">
          {data.recent_signals.map((sig, idx) => (
            <div
              key={`${sig.scheme_id}-${idx}`}
              className="flex items-start gap-3 p-3 rounded-lg bg-slate-800/40 border border-slate-800 hover:border-slate-700 transition-colors"
            >
              <div className="mt-0.5">
                {sig.arrears_score >= 80 ? (
                  <FireIcon className="w-4 h-4 text-red-400" />
                ) : (
                  <ExclamationTriangleIcon className="w-4 h-4 text-orange-400" />
                )}
              </div>
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2 flex-wrap">
                  <Link
                    href={`/schemes?expanded=${sig.scheme_id}`}
                    className="text-sm text-white hover:text-amber-300 font-medium truncate"
                  >
                    {sig.scheme_name}
                  </Link>
                  {sig.operator && <span className="text-xs text-slate-500">· {sig.operator}</span>}
                  {bucketBadge(
                    sig.arrears_score >= 80 ? 'critical' : sig.arrears_score >= 60 ? 'distressed' : 'caution',
                    sig.arrears_score,
                  )}
                </div>
                <div className="text-xs text-slate-400 mt-0.5">{sig.summary}</div>
              </div>
              <div className="text-[11px] text-slate-500 whitespace-nowrap">{formatRelative(sig.last_checked)}</div>
            </div>
          ))}
        </div>
      </Card>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Sub-components
// ---------------------------------------------------------------------------

function KpiCard({
  label,
  value,
  sub,
  tone,
  icon,
}: {
  label: string;
  value: number;
  sub: string;
  tone: 'red' | 'orange' | 'amber' | 'purple';
  icon: React.ReactNode;
}) {
  const toneClass = {
    red: 'from-red-500/10 to-red-500/[0.02] border-red-500/30 text-red-300',
    orange: 'from-orange-500/10 to-orange-500/[0.02] border-orange-500/30 text-orange-300',
    amber: 'from-amber-500/10 to-amber-500/[0.02] border-amber-500/30 text-amber-300',
    purple: 'from-purple-500/10 to-purple-500/[0.02] border-purple-500/30 text-purple-300',
  }[tone];

  return (
    <div className={cn('rounded-lg p-4 bg-gradient-to-br border', toneClass)}>
      <div className="flex items-center justify-between">
        <span className="text-xs font-medium uppercase tracking-wide text-slate-400">{label}</span>
        <span className="opacity-70">{icon}</span>
      </div>
      <div className="mt-2 text-3xl font-semibold text-white tabular-nums">{value.toLocaleString()}</div>
      <div className="mt-1 text-[11px] text-slate-500">{sub}</div>
    </div>
  );
}

function PlayCardComponent({ play }: { play: PlayCard }) {
  return (
    <Card className="p-4">
      <div className="flex items-start justify-between">
        <div className="flex-1">
          <h3 className="text-sm font-semibold text-white">{play.title}</h3>
          <p className="text-[11px] text-slate-500 mt-0.5">{play.trigger}</p>
        </div>
        <div className="text-2xl font-semibold text-amber-300 tabular-nums">{play.count}</div>
      </div>
      {play.top.length > 0 && (
        <div className="mt-3 pt-3 border-t border-slate-800 space-y-1">
          {play.top.map((t, i) => (
            <div key={i} className="flex items-center justify-between text-xs">
              <span className="text-slate-300 truncate flex-1 mr-2">
                {(t.name as string) || (t.label as string) || '—'}
              </span>
              <span className="text-slate-500 tabular-nums">
                {t.score !== undefined && typeof t.score === 'number'
                  ? `score ${t.score.toFixed(0)}`
                  : t.count !== undefined
                  ? `${t.count}`
                  : t.avg_arrears !== undefined
                  ? `avg ${(t.avg_arrears as number).toFixed(0)}`
                  : ''}
              </span>
            </div>
          ))}
        </div>
      )}
    </Card>
  );
}
