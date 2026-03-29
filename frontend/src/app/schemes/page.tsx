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
} from '@heroicons/react/24/outline';
import { cn, formatDate, getSchemeTypeColor, getPriorityColor, getBdScoreColor, getBdScoreBarColor, getContractEndColor, formatNumber } from '@/lib/utils';
import Card from '@/components/ui/Card';
import Badge from '@/components/ui/Badge';
import SearchInput from '@/components/ui/SearchInput';
import Select from '@/components/ui/Select';
import Modal from '@/components/ui/Modal';
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

// Competitor and approach data used in detail panels (enriched by API when available)
const competitorMock: Record<string, { name: string; strength: string; weakness: string }[]> = {};
const defaultCompetitors = [
  { name: 'JLL Living', strength: 'National coverage and brand recognition', weakness: 'Higher fee structure than regional operators' },
  { name: 'Savills Management', strength: 'Strong institutional relationships', weakness: 'Less agile in operational innovation' },
];
const approachMock: Record<string, string> = {};
const defaultApproach = 'Begin with market research to understand the current operator landscape and identify specific pain points. Develop a tailored value proposition highlighting our operational excellence, technology platform, and resident satisfaction track record. Schedule introductory meetings with key stakeholders within 30 days.';

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

export default function SchemesPage() {
  const [schemes, setSchemes] = useState<SchemeRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState('');
  const [sortBy, setSortBy] = useState('bd_score');
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [typeFilter, setTypeFilter] = useState('');
  const [pipelineModalScheme, setPipelineModalScheme] = useState<SchemeRow | null>(null);

  useEffect(() => {
    const params: Record<string, string> = { limit: '500' };
    if (search) params.search = search;
    if (typeFilter) params.scheme_type = typeFilter;

    api.get('/v2/schemes', { params })
      .then(res => {
        const data = res.data;
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
        }));
        setSchemes(items);
        setLoading(false);
      })
      .catch(() => setLoading(false));
  }, [search, typeFilter]);

  const sortedSchemes = [...schemes]
    .filter((s) => {
      if (typeFilter && s.scheme_type !== typeFilter) return false;
      if (!search) return true;
      const q = search.toLowerCase();
      return s.name.toLowerCase().includes(q) || s.operator.toLowerCase().includes(q) || s.council.toLowerCase().includes(q);
    })
    .sort((a, b) => {
      if (sortBy === 'bd_score') return (b.bd_score ?? 0) - (a.bd_score ?? 0);
      if (sortBy === 'contract_end') {
        const aTime = a.contract_end ? new Date(a.contract_end).getTime() : Infinity;
        const bTime = b.contract_end ? new Date(b.contract_end).getTime() : Infinity;
        return aTime - bTime;
      }
      if (sortBy === 'performance') return (a.performance ?? 999) - (b.performance ?? 999);
      return 0;
    });

  const totalSchemesCount = schemes.length;
  const totalUnits = schemes.reduce((sum, s) => sum + (s.units ?? 0), 0);
  const schemesWithBd = schemes.filter((s) => s.bd_score !== null);
  const avgBdScore = schemesWithBd.length > 0 ? Math.round(schemesWithBd.reduce((sum, s) => sum + (s.bd_score ?? 0), 0) / schemesWithBd.length) : 0;
  const contractsAtRisk = schemes.filter((s) => {
    if (!s.contract_end) return false;
    const days = getDaysRemaining(s.contract_end);
    return days !== null && days > 0 && days < 180;
  }).length;

  // Dynamic scheme type options from actual data
  const schemeTypeOptions = Array.from(new Set(schemes.map((s) => s.scheme_type).filter(Boolean)))
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
      <div className="flex flex-wrap items-center gap-3">
        <SearchInput placeholder="Search schemes, operators, councils..." onChange={setSearch} className="w-72" />
        <Select
          options={[
            { value: 'bd_score', label: 'Sort by BD Score' },
            { value: 'contract_end', label: 'Sort by Contract End' },
            { value: 'performance', label: 'Sort by Performance' },
          ]}
          value={sortBy}
          onChange={setSortBy}
          className="w-52"
        />
        <Select
          options={schemeTypeOptions}
          value={typeFilter}
          onChange={setTypeFilter}
          placeholder="All Types"
          className="w-48"
        />
        {(typeFilter || search) && (
          <button
            onClick={() => { setTypeFilter(''); setSearch(''); }}
            className="text-xs text-slate-400 hover:text-white px-2 py-1"
          >
            Clear filters
          </button>
        )}
      </div>

      {/* Table */}
      <Card noPadding>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-slate-700">
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase w-8"></th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase">Name</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase">Operator</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase">Type</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase">Units</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase">Contract End</th>
                <th className="px-4 py-3 text-center text-xs font-medium text-slate-400 uppercase">Perf.</th>
                <th className="px-4 py-3 text-center text-xs font-medium text-slate-400 uppercase">Satisf.</th>
                <th className="px-4 py-3 text-center text-xs font-medium text-slate-400 uppercase">BD Score</th>
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
                          <button
                            onClick={(e) => { e.stopPropagation(); setPipelineModalScheme(scheme); }}
                            className="inline-flex items-center gap-1 px-2.5 py-1 text-[11px] font-medium text-blue-400 bg-blue-500/10 border border-blue-500/30 rounded-full hover:bg-blue-500/20 transition-colors"
                          >
                            <PlusCircleIcon className="w-3.5 h-3.5" />
                            Add to Pipeline
                          </button>
                        ) : (
                          <span className="text-[11px] text-slate-600">No operator</span>
                        )}
                      </td>
                    </tr>

                    {/* Expanded row */}
                    {expandedId === scheme.id && (
                      <tr>
                        <td colSpan={12} className="px-8 py-6 bg-slate-800/60">
                          <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                            {/* Scheme Details */}
                            <div className="space-y-4">
                              <h4 className="text-sm font-semibold text-white flex items-center gap-2">
                                <span className="w-1 h-4 bg-blue-500 rounded-full" />
                                Scheme Details
                              </h4>
                              <div className="space-y-2 text-sm">
                                <div className="flex justify-between"><span className="text-slate-500">Address</span><span className="text-slate-300 text-right text-xs">{scheme.address || '--'}</span></div>
                                <div className="flex justify-between"><span className="text-slate-500">Postcode</span><span className="text-slate-300 font-mono text-xs">{scheme.postcode || '--'}</span></div>
                                <div className="flex justify-between"><span className="text-slate-500">Owner</span><span className="text-slate-300 text-right text-xs">{scheme.owner || '--'}</span></div>
                                <div className="flex justify-between"><span className="text-slate-500">Asset Manager</span><span className="text-slate-300 text-right text-xs">{scheme.asset_manager || '--'}</span></div>
                                <div className="flex justify-between"><span className="text-slate-500">Landlord</span><span className="text-slate-300 text-right text-xs">{scheme.landlord || '--'}</span></div>
                                <div className="flex justify-between"><span className="text-slate-500">Occupancy Rate</span><span className="text-slate-300">{scheme.occupancy_rate !== null ? `${scheme.occupancy_rate}%` : '--'}</span></div>
                                <div className="flex justify-between"><span className="text-slate-500">Revenue/Unit</span><span className="text-slate-300">{scheme.revenue_per_unit !== null ? `£${scheme.revenue_per_unit}/mo` : '--'}</span></div>
                                <div className="flex justify-between"><span className="text-slate-500">Contract Start</span><span className="text-slate-300">{scheme.contract_start ? formatDate(scheme.contract_start) : '--'}</span></div>
                                <div className="flex justify-between"><span className="text-slate-500">Contract End</span><span className={cn('font-medium', getContractEndColor(scheme.contract_end))}>{scheme.contract_end ? formatDate(scheme.contract_end) : 'Not set'}</span></div>
                              </div>
                            </div>

                            {/* BD Score Breakdown - horizontal bars */}
                            <div className="space-y-4">
                              <h4 className="text-sm font-semibold text-white flex items-center gap-2">
                                <span className="w-1 h-4 bg-violet-500 rounded-full" />
                                BD Score Breakdown
                              </h4>
                              <div className="space-y-2.5">
                                {Object.entries(scheme.score_breakdown).map(([key, val]) => (
                                  <div key={key}>
                                    <div className="flex items-center justify-between mb-1">
                                      <span className="text-[11px] text-slate-400 capitalize">{key.replace(/_/g, ' ')}</span>
                                      <span className="text-[11px] font-semibold text-slate-300">{val}</span>
                                    </div>
                                    <div className="h-1.5 bg-slate-700 rounded-full overflow-hidden">
                                      <div
                                        className={cn(
                                          'h-full rounded-full transition-all duration-700',
                                          val > 80 ? 'bg-gradient-to-r from-red-600 to-red-400' :
                                          val > 50 ? 'bg-gradient-to-r from-amber-600 to-amber-400' :
                                          'bg-gradient-to-r from-emerald-600 to-emerald-400'
                                        )}
                                        style={{ width: `${val}%` }}
                                      />
                                    </div>
                                  </div>
                                ))}
                              </div>
                            </div>

                            {/* Competitor Analysis */}
                            <div className="space-y-4">
                              <h4 className="text-sm font-semibold text-white flex items-center gap-2">
                                <span className="w-1 h-4 bg-amber-500 rounded-full" />
                                Competitor Analysis
                              </h4>
                              <div className="space-y-3">
                                {(competitorMock[scheme.id] || defaultCompetitors).map((comp, idx) => (
                                  <div key={idx} className="bg-slate-700/30 rounded-lg p-3 space-y-1.5">
                                    <p className="text-xs font-semibold text-white">{comp.name}</p>
                                    <div className="flex items-start gap-1.5">
                                      <span className="text-[10px] text-emerald-400 font-medium flex-shrink-0">+</span>
                                      <p className="text-[11px] text-slate-400">{comp.strength}</p>
                                    </div>
                                    <div className="flex items-start gap-1.5">
                                      <span className="text-[10px] text-red-400 font-medium flex-shrink-0">-</span>
                                      <p className="text-[11px] text-slate-400">{comp.weakness}</p>
                                    </div>
                                  </div>
                                ))}
                              </div>
                            </div>
                          </div>

                          {/* Contract History */}
                          <ContractTimeline schemeId={scheme.id} />

                          {/* Recommended Approach */}
                          <div className="mt-6 flex items-start gap-6">
                            <div className="flex-1">
                              <h4 className="text-sm font-semibold text-white mb-3 flex items-center gap-2">
                                <SparklesIcon className="w-4 h-4 text-amber-400" />
                                AI Recommended Approach
                              </h4>
                              <div className="bg-gradient-to-br from-amber-500/[0.06] to-orange-500/[0.06] border border-amber-500/20 rounded-lg p-4">
                                <p className="text-sm text-slate-300 leading-relaxed">
                                  {approachMock[scheme.id] || defaultApproach}
                                </p>
                              </div>
                            </div>
                            <div className="flex-shrink-0 pt-8">
                              {scheme.pipeline_opportunity_id ? (
                                <a
                                  href="/pipeline"
                                  className="inline-flex items-center gap-2 px-4 py-2.5 text-sm font-medium text-emerald-400 bg-emerald-500/10 border border-emerald-500/30 rounded-lg hover:bg-emerald-500/20 transition-colors"
                                >
                                  <CheckCircleIcon className="w-5 h-5" />
                                  View in Pipeline
                                </a>
                              ) : scheme.operator_company_id ? (
                                <button
                                  onClick={(e) => { e.stopPropagation(); setPipelineModalScheme(scheme); }}
                                  className="inline-flex items-center gap-2 px-4 py-2.5 text-sm font-medium text-white bg-gradient-to-r from-blue-600 to-purple-600 hover:from-blue-500 hover:to-purple-500 rounded-lg transition-all shadow-lg shadow-blue-500/20"
                                >
                                  <PlusCircleIcon className="w-5 h-5" />
                                  Add to Pipeline
                                </button>
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
