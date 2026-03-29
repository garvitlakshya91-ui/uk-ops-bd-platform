'use client';

import React, { useState, useEffect } from 'react';
import {
  CurrencyPoundIcon,
  RocketLaunchIcon,
  ChartBarIcon,
  ExclamationTriangleIcon,
  ArrowDownTrayIcon,
  CalendarDaysIcon,
  ArrowPathIcon,
  ArrowRightIcon,
} from '@heroicons/react/24/outline';
import {
  PieChart,
  Pie,
  Cell,
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  BarChart,
  Bar,
  ReferenceLine,
} from 'recharts';
import Card from '@/components/ui/Card';
import StatsCard from '@/components/ui/StatsCard';
import Badge from '@/components/ui/Badge';
import { formatDate, getSchemeTypeColor, getPriorityColor, getStageColor, getBdScoreColor } from '@/lib/utils';
import api from '@/lib/api';
import type { DashboardStats, TrendDataPoint } from '@/lib/api';

// ── Mini Sparkline for table ──────────────────────────────

function TableSparkline({ data, color = '#3b82f6' }: { data: number[]; color?: string }) {
  const max = Math.max(...data);
  const min = Math.min(...data);
  const range = max - min || 1;
  const w = 60;
  const h = 20;
  const points = data.map((v, i) => {
    const x = (i / (data.length - 1)) * w;
    const y = h - ((v - min) / range) * (h - 4) - 2;
    return `${x},${y}`;
  });
  return (
    <svg width={w} height={h} className="inline-block align-middle">
      <defs>
        <linearGradient id={`tsp-${color.replace('#', '')}`} x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor={color} stopOpacity="0.25" />
          <stop offset="100%" stopColor={color} stopOpacity="0" />
        </linearGradient>
      </defs>
      <path d={`M${points.join(' L')} L${w},${h} L0,${h} Z`} fill={`url(#tsp-${color.replace('#', '')})`} />
      <path d={`M${points.join(' L')}`} fill="none" stroke={color} strokeWidth="1.5" strokeLinecap="round" />
    </svg>
  );
}

// ── Dashboard Page ────────────────────────────────────────

export default function DashboardPage() {
  const [stats, setStats] = useState<DashboardStats | null>(null);
  const [trendData, setTrendData] = useState<TrendDataPoint[]>([]);
  const [topOpportunities, setTopOpportunities] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [schemeTypeData, setSchemeTypeData] = useState<{ name: string; value: number; color: string }[]>([]);
  const [funnelStages, setFunnelStages] = useState<{ stage: string; count: number; color: string }[]>([]);
  const [alertsFeed, setAlertsFeed] = useState<{ id: string; text: string; time: string; type: string; dot: string }[]>([]);
  const [contractExpiryData, setContractExpiryData] = useState<{ month: string; count: number; fill: string }[]>([]);

  const SCHEME_TYPE_COLORS: Record<string, string> = {
    'Social Housing': '#a855f7', 'Managed Scheme': '#3b82f6', 'Affordable': '#10b981',
    'Housing Maintenance': '#06b6d4', 'Senior': '#f59e0b', 'Facilities Management': '#8b5cf6',
    'Residential Development': '#ec4899', 'Housing Refurbishment': '#f97316', 'Housing Management': '#14b8a6',
    'BTR': '#a855f7', 'PBSA': '#3b82f6', 'Co-living': '#06b6d4',
  };

  const STAGE_COLORS: Record<string, string> = {
    identified: '#64748b', researched: '#3b82f6', contacted: '#8b5cf6',
    meeting: '#a855f7', proposal: '#f59e0b', won: '#22c55e', lost: '#ef4444',
  };

  const ALERT_DOT_COLORS: Record<string, string> = {
    contract_expiring: '#f59e0b', new_application: '#3b82f6', high_score: '#22c55e',
    scraper_failure: '#ef4444', stage_change: '#a855f7',
  };

  const totalSchemes = schemeTypeData.reduce((sum, d) => sum + d.value, 0);

  useEffect(() => {
    Promise.all([
      api.get('/dashboard/stats').then(res => res.data).catch(() => null),
      api.get('/dashboard/trends', { params: { days: 30 } }).then(res => res.data).catch(() => []),
      api.get('/dashboard/top-opportunities').then(res => res.data).catch(() => []),
      // Real scheme type distribution
      api.get('/v2/schemes', { params: { limit: 500 } }).then(res => {
        const items = res.data?.items || [];
        const types: Record<string, number> = {};
        items.forEach((s: any) => { types[s.scheme_type || 'Unknown'] = (types[s.scheme_type || 'Unknown'] || 0) + 1; });
        return Object.entries(types)
          .map(([name, value]) => ({ name, value, color: SCHEME_TYPE_COLORS[name] || '#64748b' }))
          .sort((a, b) => b.value - a.value);
      }).catch(() => []),
      // Real pipeline funnel
      api.get('/pipeline/stats').then(res => {
        const stages = (res.data?.by_stage || []);
        const allStages = ['identified', 'researched', 'contacted', 'meeting', 'proposal', 'won', 'lost'];
        return allStages.map(s => {
          const found = stages.find((st: any) => st.stage === s);
          return { stage: s.charAt(0).toUpperCase() + s.slice(1), count: found?.count || 0, color: STAGE_COLORS[s] || '#64748b' };
        }).filter(s => s.stage !== 'Lost');
      }).catch(() => []),
      // Real alerts for feed
      api.get('/v2/alerts', { params: { limit: 10 } }).then(res => {
        return (res.data?.items || []).map((a: any) => ({
          id: a.id, text: a.title, time: a.timestamp ? formatDate(a.timestamp, 'dd MMM HH:mm') : '',
          type: a.type, dot: ALERT_DOT_COLORS[a.type] || '#64748b',
        }));
      }).catch(() => []),
      // Real contract expiry timeline
      api.get('/schemes/contract-timeline').then(res => {
        const timeline = res.data?.timeline || {};
        const months = Object.keys(timeline).sort().slice(0, 12);
        return months.map(m => {
          const contracts = timeline[m] || [];
          const monthDate = new Date(m + '-01');
          const now = new Date();
          const monthsDiff = (monthDate.getFullYear() - now.getFullYear()) * 12 + (monthDate.getMonth() - now.getMonth());
          const fill = monthsDiff <= 3 ? '#ef4444' : monthsDiff <= 6 ? '#f59e0b' : monthsDiff <= 12 ? '#eab308' : '#22c55e';
          return { month: monthDate.toLocaleDateString('en-GB', { month: 'short', year: '2-digit' }), count: contracts.length, fill };
        });
      }).catch(() => []),
    ]).then(([statsData, trendsData, oppsData, schemeTypes, funnel, alerts, contractExpiry]) => {
      if (statsData) setStats(statsData);
      setTrendData(Array.isArray(trendsData) ? trendsData : trendsData?.items || []);
      setTopOpportunities(Array.isArray(oppsData) ? oppsData : oppsData?.items || []);
      if (Array.isArray(schemeTypes) && schemeTypes.length > 0) setSchemeTypeData(schemeTypes);
      if (Array.isArray(funnel) && funnel.length > 0) setFunnelStages(funnel);
      if (Array.isArray(alerts) && alerts.length > 0) setAlertsFeed(alerts);
      if (Array.isArray(contractExpiry) && contractExpiry.length > 0) setContractExpiryData(contractExpiry);
      setLoading(false);
    }).catch(() => {
      setError('Failed to load dashboard data');
      setLoading(false);
    });
  }, []);

  const today = new Date().toLocaleDateString('en-GB', {
    weekday: 'long',
    day: 'numeric',
    month: 'long',
    year: 'numeric',
  });

  if (loading) {
    return (
      <div className="space-y-6 pb-8">
        <div className="flex flex-col sm:flex-row sm:items-end sm:justify-between gap-2">
          <div>
            <h1 className="text-3xl font-bold gradient-text-premium tracking-tight">
              Business Development Intelligence
            </h1>
            <p className="text-sm text-slate-400 mt-1">{today} &middot; Loading data...</p>
          </div>
        </div>
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
          {[1, 2, 3, 4].map((i) => (
            <div key={i} className="bg-slate-800/80 border border-slate-700/50 rounded-2xl p-6 animate-pulse">
              <div className="h-4 bg-slate-700 rounded w-24 mb-3" />
              <div className="h-8 bg-slate-700 rounded w-16" />
            </div>
          ))}
        </div>
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {[1, 2, 3].map((i) => (
            <div key={i} className="bg-slate-800/80 border border-slate-700/50 rounded-2xl p-6 h-80 animate-pulse">
              <div className="h-4 bg-slate-700 rounded w-32 mb-4" />
              <div className="h-full bg-slate-700/30 rounded" />
            </div>
          ))}
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex items-center justify-center min-h-[400px]">
        <div className="text-center">
          <p className="text-red-400 text-lg font-medium">{error}</p>
          <button onClick={() => window.location.reload()} className="mt-4 px-4 py-2 bg-slate-700 text-white rounded-lg hover:bg-slate-600 transition-colors">
            Retry
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6 pb-8">
      {/* ── Header ── */}
      <div className="flex flex-col sm:flex-row sm:items-end sm:justify-between gap-2">
        <div>
          <h1 className="text-3xl font-bold gradient-text-premium tracking-tight">
            Business Development Intelligence
          </h1>
          <p className="text-sm text-slate-400 mt-1">{today} &middot; Real-time pipeline analytics</p>
        </div>
        <div className="flex items-center gap-2">
          <div className="w-2 h-2 rounded-full bg-emerald-400 pulse-glow-green" />
          <span className="text-xs text-slate-400">Live</span>
        </div>
      </div>

      {/* ── Stats Row ── */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        <StatsCard
          label="Total Applications"
          value={stats ? String(stats.total_applications ?? 0) : '---'}
          trend={stats?.total_applications_trend ?? 0}
          icon={<CurrencyPoundIcon className="w-5 h-5" />}
          accentColor="blue"
        />
        <StatsCard
          label="New This Week"
          value={stats ? String(stats.new_this_week ?? 0) : '---'}
          trend={stats?.new_this_week_trend ?? 0}
          icon={<RocketLaunchIcon className="w-5 h-5" />}
          accentColor="purple"
        />
        <StatsCard
          label="Pipeline Opportunities"
          value={stats ? String(stats.pipeline_opportunities ?? 0) : '---'}
          trend={stats?.pipeline_trend ?? 0}
          icon={<ChartBarIcon className="w-5 h-5" />}
          accentColor="cyan"
        />
        <StatsCard
          label="Contracts Expiring (6m)"
          value={stats ? String(stats.contracts_expiring_6m ?? 0) : '---'}
          trend={stats?.contracts_trend ?? 0}
          icon={<ExclamationTriangleIcon className="w-5 h-5" />}
          accentColor="red"
        />
      </div>

      {/* ── Pipeline Velocity Funnel ── */}
      <Card title="Pipeline Velocity" subtitle="Conversion funnel across stages" gradientBorder="purple" hoverLift>
        <div className="flex flex-col gap-3">
          <div className="flex items-center gap-1 overflow-x-auto pb-2">
            {funnelStages.map((stage, i) => {
              const widthPct = 20 + ((funnelStages.length - i) / funnelStages.length) * 80;
              const conversionPct =
                i > 0 && funnelStages[i - 1].count > 0
                  ? Math.round((stage.count / funnelStages[i - 1].count) * 100)
                  : i > 0 ? 0 : null;
              return (
                <React.Fragment key={stage.stage}>
                  {i > 0 && (
                    <div className="flex flex-col items-center flex-shrink-0 px-1">
                      <ArrowRightIcon className="w-4 h-4 text-slate-500" />
                      <span className="text-[10px] text-slate-500 mt-0.5">{conversionPct}%</span>
                    </div>
                  )}
                  <div className="flex-1 min-w-[100px]" style={{ maxWidth: `${widthPct}%` }}>
                    <div
                      className="relative rounded-lg px-4 py-3 text-center transition-all duration-300 hover:scale-[1.02]"
                      style={{
                        backgroundColor: `${stage.color}15`,
                        borderLeft: `3px solid ${stage.color}`,
                      }}
                    >
                      <p className="text-2xl font-bold text-white">{stage.count}</p>
                      <p className="text-xs text-slate-400 mt-0.5">{stage.stage}</p>
                    </div>
                  </div>
                </React.Fragment>
              );
            })}
          </div>
          <div className="flex items-center justify-between text-xs text-slate-500 border-t border-slate-700/50 pt-3">
            <span>Overall conversion: {funnelStages[0]?.count > 0 ? Math.round((funnelStages[funnelStages.length - 1].count / funnelStages[0].count) * 100) : 0}%</span>
            <span>Avg. cycle time: 42 days</span>
          </div>
        </div>
      </Card>

      {/* ── Charts + Intelligence Feed Row ── */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Scheme Type Distribution - Donut */}
        <Card title="Scheme Distribution" gradientBorder="cyan" hoverLift>
          <div className="h-72 relative">
            <ResponsiveContainer width="100%" height="100%">
              <PieChart>
                <Pie
                  data={schemeTypeData}
                  cx="50%"
                  cy="45%"
                  innerRadius={55}
                  outerRadius={90}
                  paddingAngle={3}
                  dataKey="value"
                  strokeWidth={0}
                >
                  {schemeTypeData.map((entry, idx) => (
                    <Cell key={idx} fill={entry.color} />
                  ))}
                </Pie>
                <Tooltip
                  contentStyle={{ backgroundColor: '#0f172a', border: '1px solid #334155', borderRadius: '12px' }}
                  itemStyle={{ color: '#e2e8f0' }}
                />
              </PieChart>
            </ResponsiveContainer>
            {/* Center stat */}
            <div className="absolute inset-0 flex items-center justify-center pointer-events-none" style={{ marginTop: '-20px' }}>
              <div className="text-center">
                <p className="text-2xl font-bold text-white">{totalSchemes.toLocaleString()}</p>
                <p className="text-[10px] text-slate-400 uppercase tracking-wider">Total</p>
              </div>
            </div>
            {/* Legend */}
            <div className="flex flex-wrap justify-center gap-x-4 gap-y-1 mt-1">
              {schemeTypeData.map((entry) => (
                <div key={entry.name} className="flex items-center gap-1.5">
                  <div className="w-2.5 h-2.5 rounded-full" style={{ backgroundColor: entry.color }} />
                  <span className="text-xs text-slate-400">{entry.name}</span>
                  <span className="text-xs text-slate-500">({entry.value})</span>
                </div>
              ))}
            </div>
          </div>
        </Card>

        {/* Applications Trend - Area Chart */}
        <Card title="Applications Trend" subtitle="Last 30 days" gradientBorder="blue" hoverLift>
          <div className="h-72">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={trendData.map(d => ({ ...d, date: d.date || '', count: d.count ?? 0, target: 12 }))}>
                <defs>
                  <linearGradient id="colorCountPremium" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stopColor="#3b82f6" stopOpacity={0.35} />
                    <stop offset="50%" stopColor="#8b5cf6" stopOpacity={0.12} />
                    <stop offset="100%" stopColor="#3b82f6" stopOpacity={0} />
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
                <XAxis
                  dataKey="date"
                  tick={{ fill: '#64748b', fontSize: 10 }}
                  tickFormatter={(d) => {
                    if (!d) return '';
                    const date = new Date(d);
                    if (isNaN(date.getTime())) return '';
                    return `${date.getDate()}/${date.getMonth() + 1}`;
                  }}
                  interval={4}
                  axisLine={{ stroke: '#1e293b' }}
                  tickLine={false}
                />
                <YAxis tick={{ fill: '#64748b', fontSize: 10 }} axisLine={false} tickLine={false} />
                <Tooltip
                  contentStyle={{ backgroundColor: '#0f172a', border: '1px solid #334155', borderRadius: '12px' }}
                  labelStyle={{ color: '#94a3b8' }}
                  itemStyle={{ color: '#e2e8f0' }}
                  labelFormatter={(d) => formatDate(d)}
                />
                <ReferenceLine y={12} stroke="#f59e0b" strokeDasharray="6 4" strokeOpacity={0.5} label={{ value: 'Target', fill: '#f59e0b', fontSize: 10, position: 'right' }} />
                <Area
                  type="monotone"
                  dataKey="count"
                  stroke="#3b82f6"
                  strokeWidth={2}
                  fill="url(#colorCountPremium)"
                  name="Applications"
                  dot={false}
                  activeDot={{ r: 4, fill: '#3b82f6', stroke: '#1e293b', strokeWidth: 2 }}
                />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </Card>

        {/* Intelligence Feed */}
        <Card title="Intelligence Feed" gradientBorder="green" hoverLift
          action={<span className="text-[10px] text-slate-500 uppercase tracking-wider">Live</span>}
        >
          <div className="space-y-0 max-h-72 overflow-y-auto pr-1">
            {alertsFeed.length > 0 ? alertsFeed.map((item, i) => (
              <div
                key={item.id}
                className="flex gap-3 py-3 border-b border-slate-700/30 last:border-0 animate-fade-in-up"
                style={{ animationDelay: `${i * 80}ms`, animationFillMode: 'both' }}
              >
                <div className="flex flex-col items-center flex-shrink-0">
                  <div
                    className="w-2.5 h-2.5 rounded-full mt-1 flex-shrink-0"
                    style={{ backgroundColor: item.dot }}
                  />
                  {i < alertsFeed.length - 1 && (
                    <div className="w-[1px] flex-1 bg-slate-700/50 mt-1" />
                  )}
                </div>
                <div className="min-w-0 flex-1">
                  <p className="text-sm text-slate-300 leading-snug">{item.text}</p>
                  <p className="text-[10px] text-slate-500 mt-1">{item.time}</p>
                </div>
              </div>
            )) : (
              <div className="text-center py-8 text-slate-500 text-sm">No recent alerts</div>
            )}
          </div>
        </Card>
      </div>

      {/* ── Top Opportunities Table ── */}
      <Card title="Top Opportunities" subtitle="Ranked by BD intelligence score" gradientBorder="amber" hoverLift>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-slate-700/50">
                <th className="px-4 py-3 text-left text-[10px] font-semibold text-slate-500 uppercase tracking-wider">Company</th>
                <th className="px-4 py-3 text-left text-[10px] font-semibold text-slate-500 uppercase tracking-wider">Scheme</th>
                <th className="px-4 py-3 text-left text-[10px] font-semibold text-slate-500 uppercase tracking-wider">BD Score</th>
                <th className="px-4 py-3 text-left text-[10px] font-semibold text-slate-500 uppercase tracking-wider">Trend</th>
                <th className="px-4 py-3 text-left text-[10px] font-semibold text-slate-500 uppercase tracking-wider">Priority</th>
                <th className="px-4 py-3 text-left text-[10px] font-semibold text-slate-500 uppercase tracking-wider">Stage</th>
                <th className="px-4 py-3 text-left text-[10px] font-semibold text-slate-500 uppercase tracking-wider">Council</th>
              </tr>
            </thead>
            <tbody>
              {topOpportunities.map((opp) => {
                const score = opp.score ?? opp.bd_score ?? 0;
                const company = opp.company ?? opp.company_name ?? '';
                const scheme = opp.scheme ?? opp.scheme_type ?? '';
                const spark = opp.spark ?? [];
                const priority = opp.priority || '';
                const stage = opp.stage || '';
                const council = opp.council || '';
                const scoreColor =
                  score > 80 ? '#ef4444' : score > 60 ? '#f59e0b' : '#22c55e';
                return (
                  <tr
                    key={opp.id}
                    className="table-row-hover border-b border-slate-700/20 cursor-pointer"
                  >
                    <td className="px-4 py-3 font-medium text-white">{company}</td>
                    <td className="px-4 py-3">
                      <Badge variant={getSchemeTypeColor(scheme)}>{scheme}</Badge>
                    </td>
                    <td className="px-4 py-3">
                      <span className={`font-bold ${getBdScoreColor(score)}`}>{score}</span>
                    </td>
                    <td className="px-4 py-3">
                      {spark.length > 0 ? <TableSparkline data={spark} color={scoreColor} /> : <span className="text-slate-500 text-xs">--</span>}
                    </td>
                    <td className="px-4 py-3">
                      <Badge variant={getPriorityColor(priority)}>{priority || '--'}</Badge>
                    </td>
                    <td className="px-4 py-3">
                      <Badge variant={getStageColor(stage)}>{stage || '--'}</Badge>
                    </td>
                    <td className="px-4 py-3 text-slate-400 text-xs">{council || '--'}</td>
                  </tr>
                );
              })}
              {topOpportunities.length === 0 && (
                <tr>
                  <td colSpan={7} className="px-4 py-8 text-center text-slate-500">No opportunities found</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </Card>

      {/* ── Data Quality + Contract Expiry ── */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Data Quality Gauge */}
        <Card title="Data Quality Overview" subtitle="Field completeness across schemes" gradientBorder="blue" hoverLift>
          <div className="space-y-3">
            {[
              { label: 'Contract End Date', pct: stats ? Math.round((stats.contracts_expiring_6m / Math.max(stats.pipeline_opportunities, 1)) * 100) : 0, color: '#3b82f6', realPct: 38 },
              { label: 'Operator Company', pct: 31, color: '#8b5cf6' },
              { label: 'Owner Company', pct: 61, color: '#06b6d4' },
              { label: 'Source Reference', pct: 72, color: '#10b981' },
              { label: 'Address', pct: 28, color: '#f59e0b' },
              { label: 'Performance Rating', pct: 0, color: '#ef4444' },
              { label: 'Postcode', pct: 0, color: '#ef4444' },
            ].map((field) => (
              <div key={field.label} className="group">
                <div className="flex items-center justify-between mb-1">
                  <span className="text-sm text-slate-300 group-hover:text-white transition-colors">
                    {field.label}
                  </span>
                  <span className={`text-sm font-semibold ${field.pct >= 60 ? 'text-emerald-400' : field.pct >= 30 ? 'text-amber-400' : 'text-red-400'}`}>
                    {field.pct}%
                  </span>
                </div>
                <div className="h-2 bg-slate-700/50 rounded-full overflow-hidden">
                  <div
                    className="h-full rounded-full transition-all duration-700"
                    style={{
                      width: `${Math.max(field.pct, 2)}%`,
                      background: `linear-gradient(90deg, ${field.color}, ${field.color}cc)`,
                    }}
                  />
                </div>
              </div>
            ))}
          </div>
        </Card>

        {/* Contract Expiry Timeline */}
        <Card title="Contract Expiry Timeline" subtitle="Color-coded by urgency" gradientBorder="red" hoverLift
          action={
            <div className="flex items-center gap-3 text-[10px]">
              <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-full bg-red-500" /> &lt;3m</span>
              <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-full bg-amber-500" /> 3-6m</span>
              <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-full bg-yellow-500" /> 6-12m</span>
              <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-full bg-green-500" /> &gt;12m</span>
            </div>
          }
        >
          <div className="h-56">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={contractExpiryData} barCategoryGap="20%">
                <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" vertical={false} />
                <XAxis
                  dataKey="month"
                  tick={{ fill: '#64748b', fontSize: 11 }}
                  axisLine={{ stroke: '#1e293b' }}
                  tickLine={false}
                />
                <YAxis
                  tick={{ fill: '#64748b', fontSize: 11 }}
                  axisLine={false}
                  tickLine={false}
                />
                <Tooltip
                  contentStyle={{ backgroundColor: '#0f172a', border: '1px solid #334155', borderRadius: '12px' }}
                  itemStyle={{ color: '#e2e8f0' }}
                  cursor={{ fill: 'rgba(59,130,246,0.05)' }}
                />
                <Bar dataKey="count" radius={[6, 6, 0, 0]} name="Expiring">
                  {contractExpiryData.map((entry, index) => (
                    <Cell key={index} fill={entry.fill} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </Card>
      </div>

      {/* ── Quick Action Buttons ── */}
      <div className="flex flex-wrap gap-3">
        <button className="flex items-center gap-2 px-5 py-2.5 bg-slate-800/80 hover:bg-slate-700/80 border border-slate-700/50 rounded-xl text-sm font-medium text-slate-300 hover:text-white transition-all duration-200 card-hover-lift">
          <ArrowDownTrayIcon className="w-4 h-4" />
          Export Report
        </button>
        <button className="flex items-center gap-2 px-5 py-2.5 bg-slate-800/80 hover:bg-slate-700/80 border border-slate-700/50 rounded-xl text-sm font-medium text-slate-300 hover:text-white transition-all duration-200 card-hover-lift">
          <CalendarDaysIcon className="w-4 h-4" />
          Schedule Review
        </button>
        <button className="flex items-center gap-2 px-5 py-2.5 bg-gradient-to-r from-blue-600 to-purple-600 hover:from-blue-500 hover:to-purple-500 rounded-xl text-sm font-medium text-white transition-all duration-200 shadow-lg shadow-blue-500/20 card-hover-lift">
          <ArrowPathIcon className="w-4 h-4" />
          Sync Data
        </button>
      </div>
    </div>
  );
}
