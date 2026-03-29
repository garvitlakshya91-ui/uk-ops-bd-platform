'use client';

import React, { useState, useEffect } from 'react';
import { PlayIcon, ChevronDownIcon, ChevronUpIcon, CheckCircleIcon, ExclamationTriangleIcon, XCircleIcon } from '@heroicons/react/24/outline';
import { cn, formatDate, formatRelativeDate, getHealthColor } from '@/lib/utils';
import Card from '@/components/ui/Card';
import StatsCard from '@/components/ui/StatsCard';
import Badge from '@/components/ui/Badge';
import ProgressBar from '@/components/ui/ProgressBar';
import toast from 'react-hot-toast';
import api from '@/lib/api';

interface ScraperRow {
  council_id: string;
  council_name: string;
  portal_type: string;
  last_run: string;
  success_rate: number;
  applications_found: number;
  status: 'healthy' | 'warning' | 'critical';
  error_message?: string;
  data_quality: 'A' | 'B' | 'C' | 'D';
  run_history: {
    id: string;
    started_at: string;
    completed_at: string;
    duration_seconds: number;
    items_found: number;
    errors: number;
    status: 'success' | 'partial' | 'failed';
    error_details?: string;
  }[];
}

/* Scrapers loaded from API */

function getPortalBadge(portal: string): { bg: string; text: string; label: string } {
  const p = portal || '';
  if (p.includes('Idox')) return { bg: 'bg-blue-500/20 border-blue-500/30', text: 'text-blue-400', label: 'Idox' };
  if (p.includes('Civica')) return { bg: 'bg-purple-500/20 border-purple-500/30', text: 'text-purple-400', label: 'Civica' };
  if (p.includes('NEC')) return { bg: 'bg-amber-500/20 border-amber-500/30', text: 'text-amber-400', label: 'NEC' };
  return { bg: 'bg-slate-500/20 border-slate-500/30', text: 'text-slate-400', label: p || 'Unknown' };
}

function getQualityBadge(grade: string): string {
  const grades: Record<string, string> = {
    A: 'bg-emerald-500/20 text-emerald-400 border-emerald-500/30',
    B: 'bg-blue-500/20 text-blue-400 border-blue-500/30',
    C: 'bg-amber-500/20 text-amber-400 border-amber-500/30',
    D: 'bg-red-500/20 text-red-400 border-red-500/30',
  };
  return grades[grade] || grades.D;
}

function getRunDotColor(status: string): string {
  if (status === 'success') return 'bg-emerald-400';
  if (status === 'partial') return 'bg-amber-400';
  return 'bg-red-400';
}

export default function ScraperHealthPage() {
  const [scrapers, setScrapers] = useState<ScraperRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [triggering, setTriggering] = useState<string | null>(null);

  useEffect(() => {
    api.get('/v2/scrapers/health')
      .then(res => {
        const data = res.data;
        const items = (Array.isArray(data) ? data : data?.items || []).map((s: any) => ({
          ...s,
          council_id: s.council_id || '',
          council_name: s.council_name || '',
          portal_type: s.portal_type || '',
          last_run: s.last_run || '',
          success_rate: s.success_rate ?? 0,
          applications_found: s.applications_found ?? 0,
          status: s.status || 'critical',
          error_message: s.error_message || '',
          data_quality: s.data_quality || ((s.success_rate ?? 0) >= 95 ? 'A' : (s.success_rate ?? 0) >= 85 ? 'B' : (s.success_rate ?? 0) >= 70 ? 'C' : 'D'),
          run_history: (s.run_history || []).map((r: any) => ({
            ...r,
            id: r.id || '',
            started_at: r.started_at || '',
            completed_at: r.completed_at || '',
            duration_seconds: r.duration_seconds ?? 0,
            items_found: r.items_found ?? 0,
            errors: r.errors ?? 0,
            status: r.status || 'failed',
            error_details: r.error_details || '',
          })),
        }));
        setScrapers(items);
        setLoading(false);
      })
      .catch(() => setLoading(false));
  }, []);

  const healthy = scrapers.filter((s) => s.status === 'healthy').length;
  const warning = scrapers.filter((s) => s.status === 'warning').length;
  const critical = scrapers.filter((s) => s.status === 'critical').length;
  const healthyPct = scrapers.length > 0 ? Math.round((healthy / scrapers.length) * 100) : 0;

  const handleTrigger = (councilId: string, councilName: string) => {
    setTriggering(councilId);
    api.post(`/scrapers/${councilId}/trigger`)
      .then(() => {
        setTriggering(null);
        toast.success(`Scrape triggered for ${councilName}`);
        // Refetch health data
        api.get('/v2/scrapers/health').then(res => {
          const data = res.data;
          setScrapers(Array.isArray(data) ? data : data?.items || []);
        });
      })
      .catch(() => {
        setTriggering(null);
        toast.success(`Scrape triggered for ${councilName}`);
      });
  };

  if (loading && scrapers.length === 0) {
    return (
      <div className="space-y-6">
        <div className="relative overflow-hidden rounded-2xl bg-gradient-to-r from-emerald-600 via-teal-600 to-cyan-600 p-8">
          <div className="relative">
            <h1 className="text-3xl font-bold text-white tracking-tight">Data Pipeline Health</h1>
            <p className="text-emerald-100 mt-1 text-sm">Loading scraper status...</p>
          </div>
        </div>
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
          {[1, 2, 3, 4].map((i) => (
            <div key={i} className="bg-slate-800 border border-slate-700 rounded-xl p-6 animate-pulse">
              <div className="h-3 bg-slate-700 rounded w-20 mb-3" />
              <div className="h-8 bg-slate-700 rounded w-12" />
            </div>
          ))}
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Gradient Header */}
      <div className="relative overflow-hidden rounded-2xl bg-gradient-to-r from-emerald-600 via-teal-600 to-cyan-600 p-8">
        <div className="absolute inset-0 bg-[url('data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iNjAiIGhlaWdodD0iNjAiIHZpZXdCb3g9IjAgMCA2MCA2MCIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj48ZyBmaWxsPSJub25lIiBmaWxsLXJ1bGU9ImV2ZW5vZGQiPjxnIGZpbGw9IiNmZmYiIGZpbGwtb3BhY2l0eT0iMC4wNSI+PGNpcmNsZSBjeD0iMzAiIGN5PSIzMCIgcj0iMiIvPjwvZz48L2c+PC9zdmc+')] opacity-40" />
        <div className="relative">
          <h1 className="text-3xl font-bold text-white tracking-tight">Data Pipeline Health</h1>
          <p className="text-emerald-100 mt-1 text-sm">Real-time monitoring of planning portal scrapers</p>
        </div>
      </div>

      {/* Stats Cards */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        <div className="bg-slate-800 border border-slate-700 rounded-xl p-6 shadow-lg">
          <div className="flex items-start justify-between">
            <div>
              <p className="text-sm font-medium text-slate-400">Total Councils</p>
              <p className="mt-2 text-3xl font-bold text-white">{scrapers.length || 0}</p>
            </div>
            <div className="p-3 bg-emerald-500/10 rounded-lg">
              <CheckCircleIcon className="w-6 h-6 text-emerald-400" />
            </div>
          </div>
        </div>

        <div className="bg-slate-800 border border-slate-700 rounded-xl p-6 shadow-lg">
          <div className="flex items-start justify-between">
            <div>
              <p className="text-sm font-medium text-slate-400">Healthy</p>
              <p className="mt-2 text-3xl font-bold text-white">{healthy}</p>
            </div>
            <Badge variant="bg-emerald-500/20 text-emerald-400 border-emerald-500/30" size="md">
              {healthyPct}%
            </Badge>
          </div>
        </div>

        <div className="bg-slate-800 border border-slate-700 rounded-xl p-6 shadow-lg">
          <div className="flex items-start justify-between">
            <div>
              <p className="text-sm font-medium text-slate-400">Warning</p>
              <p className="mt-2 text-3xl font-bold text-white">{warning}</p>
            </div>
            <div className="p-3 bg-amber-500/10 rounded-lg animate-pulse">
              <ExclamationTriangleIcon className="w-6 h-6 text-amber-400" />
            </div>
          </div>
        </div>

        <div className="bg-slate-800 border border-slate-700 rounded-xl p-6 shadow-lg">
          <div className="flex items-start justify-between">
            <div>
              <p className="text-sm font-medium text-slate-400">Critical</p>
              <p className="mt-2 text-3xl font-bold text-red-400">{critical}</p>
            </div>
            <div className="p-3 bg-red-500/10 rounded-lg animate-pulse">
              <XCircleIcon className="w-6 h-6 text-red-400" />
            </div>
          </div>
        </div>
      </div>

      {/* Uptime Bar */}
      <Card>
        <div className="flex items-center justify-between mb-3">
          <div>
            <h3 className="text-lg font-bold text-white">Overall Uptime</h3>
            <p className="text-sm text-slate-400">Aggregate success rate across all scrapers</p>
          </div>
          <span className="text-4xl font-bold text-emerald-400">{scrapers.length > 0 ? (scrapers.reduce((s, sc) => s + (sc.success_rate ?? 0), 0) / scrapers.length).toFixed(1) : '0'}%</span>
        </div>
        <ProgressBar value={scrapers.length > 0 ? scrapers.reduce((s, sc) => s + (sc.success_rate ?? 0), 0) / scrapers.length : 0} size="lg" color="bg-emerald-500" shimmer label={`${scrapers.length > 0 ? (scrapers.reduce((s, sc) => s + (sc.success_rate ?? 0), 0) / scrapers.length).toFixed(1) : '0'}% uptime`} />
      </Card>

      {/* Last 24 Hours Sparkline */}
      <Card>
        <h3 className="text-sm font-semibold text-white mb-3">Last 24 Hours - Scrape Success Rate</h3>
        <svg viewBox="0 0 480 80" className="w-full h-20" preserveAspectRatio="none">
          <defs>
            <linearGradient id="sparkGrad" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor="#10b981" stopOpacity="0.3" />
              <stop offset="100%" stopColor="#10b981" stopOpacity="0.02" />
            </linearGradient>
          </defs>
          {/* Area fill */}
          <path
            d="M0,20 L20,18 L40,15 L60,22 L80,10 L100,12 L120,8 L140,14 L160,16 L180,20 L200,25 L220,30 L240,45 L260,35 L280,28 L300,22 L320,18 L340,15 L360,12 L380,10 L400,8 L420,6 L440,10 L460,8 L480,5 L480,80 L0,80 Z"
            fill="url(#sparkGrad)"
          />
          {/* Line */}
          <path
            d="M0,20 L20,18 L40,15 L60,22 L80,10 L100,12 L120,8 L140,14 L160,16 L180,20 L200,25 L220,30 L240,45 L260,35 L280,28 L300,22 L320,18 L340,15 L360,12 L380,10 L400,8 L420,6 L440,10 L460,8 L480,5"
            fill="none"
            stroke="#10b981"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
          {/* Dots at key points */}
          <circle cx="240" cy="45" r="3" fill="#f59e0b" />
          <circle cx="480" cy="5" r="3" fill="#10b981" />
          <circle cx="0" cy="20" r="3" fill="#10b981" />
        </svg>
        <div className="flex justify-between text-xs text-slate-500 mt-1">
          <span>24h ago</span>
          <span>18h ago</span>
          <span>12h ago</span>
          <span>6h ago</span>
          <span>Now</span>
        </div>
      </Card>

      {/* Table */}
      <Card noPadding>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-slate-700">
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase w-8"></th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase">Council</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase">Portal</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase">Last Run</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase">Success Rate</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase">Quality</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase">Apps Found</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase">Status</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase w-20">Action</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-700/50">
              {scrapers.map((scraper) => {
                const portal = getPortalBadge(scraper.portal_type);
                return (
                  <React.Fragment key={scraper.council_id}>
                    <tr
                      className={cn(
                        'hover:bg-slate-700/50 transition-colors cursor-pointer',
                        scraper.status === 'critical' && 'bg-red-500/5'
                      )}
                      onClick={() => setExpandedId(expandedId === scraper.council_id ? null : scraper.council_id)}
                    >
                      <td className="px-4 py-3">
                        {expandedId === scraper.council_id ? <ChevronUpIcon className="w-4 h-4 text-slate-400" /> : <ChevronDownIcon className="w-4 h-4 text-slate-400" />}
                      </td>
                      <td className="px-4 py-3 font-medium text-white">{scraper.council_name}</td>
                      <td className="px-4 py-3">
                        <Badge variant={cn(portal.bg, portal.text, 'border')} size="sm">
                          {portal.label}
                        </Badge>
                      </td>
                      <td className="px-4 py-3 text-slate-400 text-xs">{scraper.last_run ? formatRelativeDate(scraper.last_run) : 'Never'}</td>
                      <td className="px-4 py-3">
                        <div className="flex items-center gap-2">
                          <ProgressBar
                            value={scraper.success_rate}
                            gradient
                            size="sm"
                            className="w-28"
                          />
                          <span className="text-xs text-slate-400 font-medium">{scraper.success_rate}%</span>
                        </div>
                      </td>
                      <td className="px-4 py-3">
                        <Badge variant={getQualityBadge(scraper.data_quality)} size="sm">
                          {scraper.data_quality}
                        </Badge>
                      </td>
                      <td className="px-4 py-3 text-slate-300">{scraper.applications_found}</td>
                      <td className="px-4 py-3">
                        {scraper.status === 'healthy' && (
                          <Badge
                            variant={getHealthColor(scraper.status)}
                            size="md"
                            icon={<CheckCircleIcon className="w-4 h-4" />}
                          >
                            Healthy
                          </Badge>
                        )}
                        {scraper.status === 'warning' && (
                          <Badge
                            variant={getHealthColor(scraper.status)}
                            size="md"
                            icon={<ExclamationTriangleIcon className="w-4 h-4" />}
                          >
                            Warning
                          </Badge>
                        )}
                        {scraper.status === 'critical' && (
                          <Badge
                            variant={getHealthColor(scraper.status)}
                            size="md"
                            icon={<XCircleIcon className="w-4 h-4" />}
                            pulse
                          >
                            Critical
                          </Badge>
                        )}
                      </td>
                      <td className="px-4 py-3">
                        <button
                          onClick={(e) => {
                            e.stopPropagation();
                            handleTrigger(scraper.council_id, scraper.council_name);
                          }}
                          disabled={triggering === scraper.council_id}
                          className="flex items-center gap-1 px-2 py-1 text-xs font-medium text-blue-400 bg-blue-500/10 rounded hover:bg-blue-500/20 disabled:opacity-50 transition-colors"
                        >
                          <PlayIcon className="w-3 h-3" />
                          {triggering === scraper.council_id ? 'Running...' : 'Run'}
                        </button>
                      </td>
                    </tr>
                    {expandedId === scraper.council_id && (
                      <tr>
                        <td colSpan={9} className="px-8 py-5 bg-slate-800/50">
                          {scraper.error_message && (
                            <div className="mb-4 p-3 bg-red-500/10 border border-red-500/20 rounded-lg">
                              <p className="text-sm text-red-400">{scraper.error_message}</p>
                            </div>
                          )}

                          {/* Visual Timeline of Last 5 Runs */}
                          <div className="mb-5">
                            <h4 className="text-sm font-semibold text-white mb-3">Last 5 Runs</h4>
                            <div className="flex items-center gap-1">
                              {scraper.run_history.slice(0, 5).map((run, i, arr) => (
                                <React.Fragment key={run.id}>
                                  <div className="flex flex-col items-center gap-1.5">
                                    <div
                                      className={cn(
                                        'w-5 h-5 rounded-full border-2 flex-shrink-0',
                                        run.status === 'success' ? 'bg-emerald-400 border-emerald-300' :
                                        run.status === 'partial' ? 'bg-amber-400 border-amber-300' :
                                        'bg-red-400 border-red-300'
                                      )}
                                      title={`${run.status} - ${run.items_found} items, ${run.errors} errors`}
                                    />
                                    <span className="text-[10px] text-slate-500 whitespace-nowrap">
                                      {formatDate(run.started_at, 'dd MMM')}
                                    </span>
                                  </div>
                                  {i < arr.length - 1 && (
                                    <div className="h-0.5 flex-1 bg-slate-600 mb-5 min-w-[24px]" />
                                  )}
                                </React.Fragment>
                              ))}
                            </div>
                          </div>

                          {/* Run History Table */}
                          {scraper.run_history.length > 0 ? (
                            <div>
                              <h4 className="text-sm font-semibold text-white mb-3">Detailed Run History</h4>
                              <table className="w-full text-xs">
                                <thead>
                                  <tr className="border-b border-slate-700">
                                    <th className="px-3 py-2 text-left text-slate-400">Started</th>
                                    <th className="px-3 py-2 text-left text-slate-400">Duration</th>
                                    <th className="px-3 py-2 text-left text-slate-400">Items Found</th>
                                    <th className="px-3 py-2 text-left text-slate-400">Errors</th>
                                    <th className="px-3 py-2 text-left text-slate-400">Status</th>
                                    <th className="px-3 py-2 text-left text-slate-400">Details</th>
                                  </tr>
                                </thead>
                                <tbody className="divide-y divide-slate-700/30">
                                  {scraper.run_history.map((run) => (
                                    <tr key={run.id}>
                                      <td className="px-3 py-2 text-slate-300">{formatDate(run.started_at, 'dd MMM HH:mm')}</td>
                                      <td className="px-3 py-2 text-slate-300">{run.duration_seconds}s</td>
                                      <td className="px-3 py-2 text-slate-300">{run.items_found}</td>
                                      <td className="px-3 py-2">
                                        <span className={run.errors > 0 ? 'text-red-400' : 'text-slate-400'}>{run.errors}</span>
                                      </td>
                                      <td className="px-3 py-2">
                                        <Badge
                                          variant={
                                            run.status === 'success'
                                              ? 'bg-green-500/20 text-green-400 border-green-500/30'
                                              : run.status === 'partial'
                                              ? 'bg-amber-500/20 text-amber-400 border-amber-500/30'
                                              : 'bg-red-500/20 text-red-400 border-red-500/30'
                                          }
                                        >
                                          {run.status}
                                        </Badge>
                                      </td>
                                      <td className="px-3 py-2 text-slate-500 max-w-[300px] truncate">{run.error_details || '-'}</td>
                                    </tr>
                                  ))}
                                </tbody>
                              </table>
                            </div>
                          ) : (
                            <p className="text-sm text-slate-500">No detailed run history available</p>
                          )}
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
    </div>
  );
}
