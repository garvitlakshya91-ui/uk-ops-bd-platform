'use client';

import React, { useState, useEffect, useCallback } from 'react';
import {
  ViewColumnsIcon,
  TableCellsIcon,
  CurrencyPoundIcon,
  ChartBarIcon,
  ClockIcon,
  PlusCircleIcon,
  SparklesIcon,
  DocumentDuplicateIcon,
  CheckCircleIcon,
} from '@heroicons/react/24/outline';
import { cn, PIPELINE_STAGES, getSchemeTypeColor, getPriorityColor, getStageColor, getBdScoreColor, formatDate, formatCurrency, formatRelativeDate } from '@/lib/utils';
import Card from '@/components/ui/Card';
import Badge from '@/components/ui/Badge';
import KanbanBoard from '@/components/ui/KanbanBoard';
import Table, { Column } from '@/components/ui/Table';
import SearchInput from '@/components/ui/SearchInput';
import Select from '@/components/ui/Select';
import Modal from '@/components/ui/Modal';
import api from '@/lib/api';
import type { PipelineOpportunity } from '@/lib/api';

// Score breakdown factors (used in detail modal when API doesn't provide them)
const scoreBreakdownMock: Record<string, { label: string; value: number }[]> = {};

// Mock recommended actions (used in detail modal when API doesn't provide them)
const recommendedActionsMock: Record<string, string[]> = {};

// Mock stage progression data (used in detail modal when API doesn't provide them)
const stageProgressionMock: Record<string, { stage: string; date: string }[]> = {};

const defaultScoreBreakdown = [
  { label: 'Market Opportunity', value: 70 },
  { label: 'Relationship Strength', value: 65 },
  { label: 'Scheme Size', value: 60 },
  { label: 'Competitive Position', value: 72 },
  { label: 'Timing Alignment', value: 68 },
];

const defaultActions = [
  'Research current operator performance and identify pain points',
  'Identify key decision-makers and build relationship map',
  'Prepare tailored capability presentation with relevant case studies',
  'Set up introductory call within the next 2 weeks',
];

const stageLabels: Record<string, string> = {
  identified: 'Identified',
  researched: 'Researched',
  contacted: 'Contacted',
  meeting: 'Meeting',
  proposal: 'Proposal',
  won: 'Won',
  lost: 'Lost',
};

const stageTimelineDotColors: Record<string, string> = {
  identified: 'bg-slate-400',
  researched: 'bg-blue-500',
  contacted: 'bg-indigo-500',
  meeting: 'bg-violet-500',
  proposal: 'bg-amber-500',
  won: 'bg-emerald-500',
  lost: 'bg-red-500',
};

const priorityDotColors: Record<string, string> = {
  high: 'bg-red-500',
  medium: 'bg-amber-500',
  low: 'bg-emerald-500',
};

const columns: Column<PipelineOpportunity>[] = [
  {
    key: 'company_name', header: 'Company', sortable: true,
    render: (item) => (
      <div className="flex items-center gap-2">
        <div className={cn('w-2 h-full absolute left-0 top-0 bottom-0 rounded-l', item.priority === 'high' ? 'bg-red-500/60' : 'bg-transparent')} />
        <span className="font-medium text-white">{item.company_name || '--'}</span>
      </div>
    ),
  },
  { key: 'scheme_type', header: 'Scheme', sortable: true, render: (item) => <Badge variant={getSchemeTypeColor(item.scheme_type || '')}>{item.scheme_type || '--'}</Badge> },
  { key: 'stage', header: 'Stage', sortable: true, render: (item) => <Badge variant={getStageColor(item.stage || '')}>{item.stage || '--'}</Badge> },
  { key: 'bd_score', header: 'BD Score', sortable: true, render: (item) => <span className={cn('font-bold', getBdScoreColor(item.bd_score ?? 0))}>{item.bd_score ?? 0}</span> },
  {
    key: 'priority', header: 'Priority', sortable: true,
    render: (item) => (
      <div className="flex items-center gap-1.5">
        <div className={cn('w-2.5 h-2.5 rounded-full', priorityDotColors[item.priority || ''] || 'bg-slate-500')} />
        <span className="text-xs text-slate-400 capitalize">{item.priority || '--'}</span>
      </div>
    ),
  },
  { key: 'council', header: 'Council', sortable: true, render: (item) => <span className="text-slate-400 text-xs">{item.council || '--'}</span> },
  { key: 'units', header: 'Units', sortable: true },
  { key: 'estimated_value', header: 'Est. Value', sortable: true, render: (item) => <span className="text-emerald-400 font-medium text-sm">{formatCurrency(item.estimated_value ?? 0)}</span> },
  { key: 'assigned_to', header: 'Assigned', sortable: true, render: (item) => <span className="text-slate-400 text-xs">{item.assigned_to || '--'}</span> },
];

export default function PipelinePage() {
  const [view, setView] = useState<'kanban' | 'table'>('kanban');
  const [search, setSearch] = useState('');
  const [schemeFilter, setSchemeFilter] = useState('');
  const [priorityFilter, setPriorityFilter] = useState('');
  const [assignedFilter, setAssignedFilter] = useState('');
  const [selectedOpp, setSelectedOpp] = useState<PipelineOpportunity | null>(null);
  const [opportunities, setOpportunities] = useState<PipelineOpportunity[]>([]);
  const [loading, setLoading] = useState(true);
  const [copiedEmail, setCopiedEmail] = useState(false);

  useEffect(() => {
    const params: Record<string, string> = {};
    if (schemeFilter) params.scheme_type = schemeFilter;
    if (priorityFilter) params.priority = priorityFilter;
    if (assignedFilter) params.assigned_to = assignedFilter;
    if (search) params.search = search;

    api.get('/v2/pipeline', { params })
      .then(res => {
        const data = res.data;
        setOpportunities(Array.isArray(data) ? data : data?.items || []);
        setLoading(false);
      })
      .catch(() => setLoading(false));
  }, [schemeFilter, priorityFilter, assignedFilter, search]);

  const filtered = opportunities.filter((opp) => {
    if (search && !(opp.company_name || '').toLowerCase().includes(search.toLowerCase()) && !(opp.council || '').toLowerCase().includes(search.toLowerCase())) return false;
    if (schemeFilter && opp.scheme_type !== schemeFilter) return false;
    if (priorityFilter && opp.priority !== priorityFilter) return false;
    if (assignedFilter && opp.assigned_to !== assignedFilter) return false;
    return true;
  });

  const totalValue = opportunities.reduce((s, o) => s + (o.estimated_value ?? 0), 0);
  const wonCount = opportunities.filter((o) => o.stage === 'won').length;
  const totalExWon = opportunities.filter((o) => o.stage !== 'identified').length;
  const winRate = totalExWon > 0 ? Math.round((wonCount / totalExWon) * 100) : 0;
  const newThisMonth = opportunities.filter((o) => {
    if (!o.created_at) return false;
    const d = new Date(o.created_at);
    if (isNaN(d.getTime())) return false;
    const now = new Date();
    return d.getMonth() === now.getMonth() && d.getFullYear() === now.getFullYear();
  }).length;

  const handleCardDrop = useCallback((cardId: string, newStage: string) => {
    setOpportunities((prev) =>
      prev.map((opp) => (opp.id === cardId ? { ...opp, stage: newStage } : opp))
    );
    // Call API to persist the stage change
    api.put(`/pipeline/${cardId}/stage?stage=${newStage}`).catch(() => {
      // Revert on failure - refetch
      api.get('/v2/pipeline').then(res => {
        const data = res.data;
        setOpportunities(Array.isArray(data) ? data : data?.items || []);
      });
    });
  }, []);

  const handleCardClick = useCallback((card: any) => {
    const opp = opportunities.find((o) => o.id === card.id);
    if (opp) setSelectedOpp(opp);
  }, [opportunities]);

  const handleCopyEmail = (email: string) => {
    navigator.clipboard.writeText(email);
    setCopiedEmail(true);
    setTimeout(() => setCopiedEmail(false), 2000);
  };

  const selectedScoreBreakdown = selectedOpp ? (scoreBreakdownMock[selectedOpp.id] || defaultScoreBreakdown) : defaultScoreBreakdown;
  const selectedActions = selectedOpp ? (recommendedActionsMock[selectedOpp.id] || defaultActions) : defaultActions;
  const selectedProgression = selectedOpp ? (stageProgressionMock[selectedOpp.id] || [{ stage: selectedOpp.stage || '', date: (selectedOpp.created_at || '').split('T')[0] }]) : [];

  if (loading && opportunities.length === 0) {
    return (
      <div className="space-y-6">
        <div>
          <h1 className="text-2xl font-bold text-white">Pipeline</h1>
          <p className="text-sm text-slate-400 mt-1">Loading opportunities...</p>
        </div>
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
          {[1, 2, 3, 4].map((i) => (
            <div key={i} className="bg-slate-800/80 border border-slate-700/80 rounded-xl px-4 py-3 animate-pulse">
              <div className="h-3 bg-slate-700 rounded w-20 mb-2" />
              <div className="h-6 bg-slate-700 rounded w-16" />
            </div>
          ))}
        </div>
        <div className="h-96 bg-slate-800/50 rounded-xl animate-pulse" />
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white">Pipeline</h1>
          <p className="text-sm text-slate-400 mt-1">Manage BD opportunities across stages</p>
        </div>
        <div className="flex items-center gap-2 bg-slate-800 rounded-lg p-1 border border-slate-700">
          <button
            onClick={() => setView('kanban')}
            className={cn('flex items-center gap-1.5 px-3 py-1.5 rounded-md text-sm font-medium transition-colors', view === 'kanban' ? 'bg-blue-600 text-white' : 'text-slate-400 hover:text-white')}
          >
            <ViewColumnsIcon className="w-4 h-4" />
            Kanban
          </button>
          <button
            onClick={() => setView('table')}
            className={cn('flex items-center gap-1.5 px-3 py-1.5 rounded-md text-sm font-medium transition-colors', view === 'table' ? 'bg-blue-600 text-white' : 'text-slate-400 hover:text-white')}
          >
            <TableCellsIcon className="w-4 h-4" />
            Table
          </button>
        </div>
      </div>

      {/* Pipeline Summary Bar */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
        <div className="bg-slate-800/80 border border-slate-700/80 rounded-xl px-4 py-3 flex items-center gap-3">
          <div className="p-2 bg-emerald-500/10 rounded-lg">
            <CurrencyPoundIcon className="w-5 h-5 text-emerald-400" />
          </div>
          <div>
            <p className="text-[10px] uppercase tracking-wider text-slate-500 font-medium">Total Value</p>
            <p className="text-lg font-bold text-white">{formatCurrency(totalValue)}</p>
          </div>
        </div>
        <div className="bg-slate-800/80 border border-slate-700/80 rounded-xl px-4 py-3 flex items-center gap-3">
          <div className="p-2 bg-blue-500/10 rounded-lg">
            <ChartBarIcon className="w-5 h-5 text-blue-400" />
          </div>
          <div>
            <p className="text-[10px] uppercase tracking-wider text-slate-500 font-medium">Win Rate</p>
            <p className="text-lg font-bold text-white">{winRate}%</p>
          </div>
        </div>
        <div className="bg-slate-800/80 border border-slate-700/80 rounded-xl px-4 py-3 flex items-center gap-3">
          <div className="p-2 bg-violet-500/10 rounded-lg">
            <ClockIcon className="w-5 h-5 text-violet-400" />
          </div>
          <div>
            <p className="text-[10px] uppercase tracking-wider text-slate-500 font-medium">Avg Time to Close</p>
            <p className="text-lg font-bold text-white">45 days</p>
          </div>
        </div>
        <div className="bg-slate-800/80 border border-slate-700/80 rounded-xl px-4 py-3 flex items-center gap-3">
          <div className="p-2 bg-amber-500/10 rounded-lg">
            <PlusCircleIcon className="w-5 h-5 text-amber-400" />
          </div>
          <div>
            <p className="text-[10px] uppercase tracking-wider text-slate-500 font-medium">This Month</p>
            <p className="text-lg font-bold text-white">+{newThisMonth} new</p>
          </div>
        </div>
      </div>

      {/* Filters */}
      <div className="flex flex-wrap items-center gap-3">
        <SearchInput placeholder="Search companies, councils..." onChange={setSearch} className="w-64" />
        <Select
          options={[
            { value: 'BTR', label: 'BTR' },
            { value: 'PBSA', label: 'PBSA' },
            { value: 'Co-living', label: 'Co-living' },
            { value: 'Senior', label: 'Senior' },
            { value: 'Affordable', label: 'Affordable' },
          ]}
          value={schemeFilter}
          onChange={setSchemeFilter}
          placeholder="All Scheme Types"
          className="w-44"
        />
        <Select
          options={[
            { value: 'high', label: 'High' },
            { value: 'medium', label: 'Medium' },
            { value: 'low', label: 'Low' },
          ]}
          value={priorityFilter}
          onChange={setPriorityFilter}
          placeholder="All Priorities"
          className="w-40"
        />
        <Select
          options={[
            { value: 'James Richardson', label: 'James Richardson' },
            { value: 'Emily Clarke', label: 'Emily Clarke' },
          ]}
          value={assignedFilter}
          onChange={setAssignedFilter}
          placeholder="All Assigned"
          className="w-48"
        />
        {(schemeFilter || priorityFilter || assignedFilter || search) && (
          <button
            onClick={() => { setSchemeFilter(''); setPriorityFilter(''); setAssignedFilter(''); setSearch(''); }}
            className="text-xs text-slate-400 hover:text-white px-2 py-1"
          >
            Clear filters
          </button>
        )}
      </div>

      {/* View */}
      {view === 'kanban' ? (
        <KanbanBoard
          stages={[...PIPELINE_STAGES]}
          cards={filtered}
          onCardClick={handleCardClick}
          onCardDrop={handleCardDrop}
        />
      ) : (
        <Card noPadding>
          <Table
            columns={columns}
            data={filtered}
            onRowClick={(item) => setSelectedOpp(item)}
            pageSize={15}
            rowClassName={(item) =>
              item.priority === 'high'
                ? 'border-l-2 border-l-red-500/60 bg-red-500/[0.03]'
                : ''
            }
          />
        </Card>
      )}

      {/* Detail Modal */}
      <Modal isOpen={!!selectedOpp} onClose={() => setSelectedOpp(null)} title={selectedOpp?.company_name || ''} size="xl">
        {selectedOpp && (
          <div className="space-y-6 max-h-[75vh] overflow-y-auto pr-1">
            {/* Top stats row */}
            <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
              <div className="bg-slate-700/30 rounded-lg p-3">
                <p className="text-[10px] uppercase tracking-wider text-slate-500 font-medium mb-1">Scheme Type</p>
                <Badge variant={getSchemeTypeColor(selectedOpp.scheme_type || '')} size="md">{selectedOpp.scheme_type || '--'}</Badge>
              </div>
              <div className="bg-slate-700/30 rounded-lg p-3">
                <p className="text-[10px] uppercase tracking-wider text-slate-500 font-medium mb-1">Stage</p>
                <Badge variant={getStageColor(selectedOpp.stage || '')} size="md">{selectedOpp.stage || '--'}</Badge>
              </div>
              <div className="bg-slate-700/30 rounded-lg p-3">
                <p className="text-[10px] uppercase tracking-wider text-slate-500 font-medium mb-1">Priority</p>
                <div className="flex items-center gap-1.5">
                  <div className={cn('w-3 h-3 rounded-full', priorityDotColors[selectedOpp.priority || ''] || 'bg-slate-500')} />
                  <span className="text-sm font-semibold text-white capitalize">{selectedOpp.priority || '--'}</span>
                </div>
              </div>
              <div className="bg-slate-700/30 rounded-lg p-3">
                <p className="text-[10px] uppercase tracking-wider text-slate-500 font-medium mb-1">BD Score</p>
                <span className={cn('text-2xl font-bold', getBdScoreColor(selectedOpp.bd_score ?? 0))}>{selectedOpp.bd_score ?? 0}</span>
              </div>
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              {/* Details */}
              <div className="space-y-4">
                <h4 className="text-sm font-semibold text-white flex items-center gap-2">
                  <span className="w-1 h-4 bg-blue-500 rounded-full" />
                  Details
                </h4>
                <div className="space-y-2 text-sm">
                  <div className="flex justify-between"><span className="text-slate-500">Council</span><span className="text-slate-200">{selectedOpp.council || '--'}</span></div>
                  <div className="flex justify-between"><span className="text-slate-500">Units</span><span className="text-slate-200">{selectedOpp.units ?? 0}</span></div>
                  <div className="flex justify-between"><span className="text-slate-500">Est. Value</span><span className="text-emerald-400 font-semibold">{formatCurrency(selectedOpp.estimated_value ?? 0)}</span></div>
                  <div className="flex justify-between"><span className="text-slate-500">Assigned To</span><span className="text-slate-200">{selectedOpp.assigned_to || '--'}</span></div>
                  <div className="flex justify-between"><span className="text-slate-500">Created</span><span className="text-slate-200">{formatDate(selectedOpp.created_at || '')}</span></div>
                  <div className="flex justify-between"><span className="text-slate-500">Last Activity</span><span className="text-slate-200">{formatRelativeDate(selectedOpp.last_activity || '')}</span></div>
                </div>
              </div>

              {/* Contact */}
              <div className="space-y-4">
                <h4 className="text-sm font-semibold text-white flex items-center gap-2">
                  <span className="w-1 h-4 bg-violet-500 rounded-full" />
                  Contact
                </h4>
                <div className="bg-slate-700/30 rounded-lg p-4 space-y-3">
                  <div className="flex items-center gap-3">
                    <div className="w-10 h-10 rounded-full bg-gradient-to-br from-violet-600 to-violet-400 flex items-center justify-center text-xs font-bold text-white">
                      {(selectedOpp.contact_name || '').split(' ').filter(Boolean).map((w) => w[0]).join('').toUpperCase() || '?'}
                    </div>
                    <div>
                      <p className="text-sm font-medium text-white">{selectedOpp.contact_name || 'No contact'}</p>
                      <p className="text-xs text-slate-400">Primary Contact</p>
                    </div>
                  </div>
                  <div className="flex items-center gap-2">
                    <a href={`mailto:${selectedOpp.contact_email || ''}`} className="text-blue-400 hover:underline text-sm flex-1 truncate">
                      {selectedOpp.contact_email || 'No email'}
                    </a>
                    <button
                      onClick={() => handleCopyEmail(selectedOpp.contact_email || '')}
                      className="p-1.5 rounded-md hover:bg-slate-600 text-slate-400 hover:text-white transition-colors flex-shrink-0"
                      title="Copy email"
                    >
                      {copiedEmail ? (
                        <CheckCircleIcon className="w-4 h-4 text-emerald-400" />
                      ) : (
                        <DocumentDuplicateIcon className="w-4 h-4" />
                      )}
                    </button>
                  </div>
                </div>
              </div>
            </div>

            {/* Notes */}
            <div>
              <h4 className="text-sm font-semibold text-white mb-2 flex items-center gap-2">
                <span className="w-1 h-4 bg-amber-500 rounded-full" />
                Notes
              </h4>
              <p className="text-sm text-slate-300 bg-slate-700/30 rounded-lg p-3">{selectedOpp.notes || 'No notes'}</p>
            </div>

            {/* Score Breakdown */}
            <div>
              <h4 className="text-sm font-semibold text-white mb-3 flex items-center gap-2">
                <span className="w-1 h-4 bg-cyan-500 rounded-full" />
                Score Breakdown
              </h4>
              <div className="space-y-2.5">
                {selectedScoreBreakdown.map((factor) => (
                  <div key={factor.label}>
                    <div className="flex items-center justify-between mb-1">
                      <span className="text-xs text-slate-400">{factor.label}</span>
                      <span className="text-xs font-semibold text-slate-300">{factor.value}%</span>
                    </div>
                    <div className="h-2 bg-slate-700 rounded-full overflow-hidden">
                      <div
                        className={cn(
                          'h-full rounded-full transition-all duration-700',
                          factor.value >= 85 ? 'bg-gradient-to-r from-red-600 to-red-400' :
                          factor.value >= 65 ? 'bg-gradient-to-r from-amber-600 to-amber-400' :
                          'bg-gradient-to-r from-emerald-600 to-emerald-400'
                        )}
                        style={{ width: `${factor.value}%` }}
                      />
                    </div>
                  </div>
                ))}
              </div>
            </div>

            {/* Stage Progression Timeline */}
            {selectedProgression.length > 0 && (
              <div>
                <h4 className="text-sm font-semibold text-white mb-3 flex items-center gap-2">
                  <span className="w-1 h-4 bg-indigo-500 rounded-full" />
                  Stage Progression
                </h4>
                <div className="relative">
                  <div className="absolute left-[7px] top-2 bottom-2 w-0.5 bg-slate-700" />
                  <div className="space-y-3">
                    {selectedProgression.map((sp, idx) => (
                      <div key={sp.stage} className="flex items-center gap-3 relative">
                        <div className={cn(
                          'w-4 h-4 rounded-full border-2 flex-shrink-0 z-10',
                          idx === selectedProgression.length - 1
                            ? `${stageTimelineDotColors[sp.stage]} border-transparent ring-2 ring-offset-1 ring-offset-slate-800 ring-current`
                            : `${stageTimelineDotColors[sp.stage]} border-transparent`
                        )} />
                        <div className="flex items-center gap-3 flex-1">
                          <Badge variant={getStageColor(sp.stage)} size="sm">
                            {stageLabels[sp.stage] || sp.stage}
                          </Badge>
                          <span className="text-xs text-slate-500">{formatDate(sp.date)}</span>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            )}

            {/* Recommended Actions */}
            <div>
              <h4 className="text-sm font-semibold text-white mb-3 flex items-center gap-2">
                <SparklesIcon className="w-4 h-4 text-amber-400" />
                AI Recommended Actions
              </h4>
              <div className="bg-gradient-to-br from-amber-500/5 to-orange-500/5 border border-amber-500/20 rounded-lg p-4 space-y-2.5">
                {selectedActions.map((action, idx) => (
                  <div key={idx} className="flex items-start gap-2.5">
                    <span className="flex-shrink-0 w-5 h-5 rounded-full bg-amber-500/20 text-amber-400 text-[10px] font-bold flex items-center justify-center mt-0.5">
                      {idx + 1}
                    </span>
                    <p className="text-sm text-slate-300">{action}</p>
                  </div>
                ))}
              </div>
            </div>

            {/* Activity Log */}
            {selectedOpp.activities && selectedOpp.activities.length > 0 && (
              <div>
                <h4 className="text-sm font-semibold text-white mb-3 flex items-center gap-2">
                  <span className="w-1 h-4 bg-emerald-500 rounded-full" />
                  Activity Log
                </h4>
                <div className="space-y-3">
                  {selectedOpp.activities.map((act) => (
                    <div key={act.id} className="flex items-start gap-3 text-sm bg-slate-700/20 rounded-lg p-3">
                      <div className="w-2 h-2 rounded-full bg-blue-500 mt-1.5 flex-shrink-0" />
                      <div className="flex-1">
                        <p className="text-slate-300">{act.description || ''}</p>
                        <p className="text-xs text-slate-500 mt-0.5">{formatDate(act.date || '', 'dd MMM yyyy HH:mm')} - {act.user || 'Unknown'}</p>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        )}
      </Modal>
    </div>
  );
}
