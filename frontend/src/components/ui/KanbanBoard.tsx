'use client';

import React, { useState } from 'react';
import { cn, getStageColor, getSchemeTypeColor, formatCurrency, formatRelativeDate } from '@/lib/utils';
import Badge from './Badge';

interface KanbanCard {
  id: string;
  company_name: string;
  scheme_type: string;
  bd_score: number;
  priority: 'high' | 'medium' | 'low';
  council: string;
  units: number;
  stage: string;
  estimated_value?: number;
  last_activity?: string;
}

interface KanbanBoardProps {
  stages: string[];
  cards: KanbanCard[];
  onCardClick?: (card: KanbanCard) => void;
  onCardDrop?: (cardId: string, newStage: string) => void;
}

const stageLabels: Record<string, string> = {
  identified: 'Identified',
  researched: 'Researched',
  contacted: 'Contacted',
  meeting: 'Meeting',
  proposal: 'Proposal',
  won: 'Won',
  lost: 'Lost',
};

const stageTopBorderColors: Record<string, string> = {
  identified: 'border-t-slate-400',
  researched: 'border-t-blue-500',
  contacted: 'border-t-indigo-500',
  meeting: 'border-t-violet-500',
  proposal: 'border-t-amber-500',
  won: 'border-t-emerald-500',
  lost: 'border-t-red-500',
};

const priorityDotColors: Record<string, string> = {
  high: 'bg-red-500',
  medium: 'bg-amber-500',
  low: 'bg-emerald-500',
};

function getCompanyInitials(name: string): string {
  return name
    .split(/\s+/)
    .slice(0, 2)
    .map((w) => w[0])
    .join('')
    .toUpperCase();
}

const companyAvatarColors = [
  'from-blue-600 to-blue-400',
  'from-purple-600 to-purple-400',
  'from-emerald-600 to-emerald-400',
  'from-amber-600 to-amber-400',
  'from-rose-600 to-rose-400',
  'from-cyan-600 to-cyan-400',
  'from-indigo-600 to-indigo-400',
  'from-teal-600 to-teal-400',
];

function getAvatarColor(name: string): string {
  let hash = 0;
  for (let i = 0; i < name.length; i++) {
    hash = name.charCodeAt(i) + ((hash << 5) - hash);
  }
  return companyAvatarColors[Math.abs(hash) % companyAvatarColors.length];
}

export default function KanbanBoard({ stages, cards, onCardClick, onCardDrop }: KanbanBoardProps) {
  const [dragOverStage, setDragOverStage] = useState<string | null>(null);
  const [draggingId, setDraggingId] = useState<string | null>(null);

  const handleDragStart = (e: React.DragEvent, cardId: string) => {
    e.dataTransfer.setData('cardId', cardId);
    setDraggingId(cardId);
  };

  const handleDragEnd = () => {
    setDraggingId(null);
    setDragOverStage(null);
  };

  const handleDragOver = (e: React.DragEvent, stage: string) => {
    e.preventDefault();
    setDragOverStage(stage);
  };

  const handleDragLeave = () => {
    setDragOverStage(null);
  };

  const handleDrop = (e: React.DragEvent, stage: string) => {
    e.preventDefault();
    const cardId = e.dataTransfer.getData('cardId');
    onCardDrop?.(cardId, stage);
    setDragOverStage(null);
    setDraggingId(null);
  };

  return (
    <div className="flex gap-4 overflow-x-auto pb-4">
      {stages.map((stage) => {
        const stageCards = cards.filter((c) => c.stage === stage);
        const totalValue = stageCards.reduce((sum, c) => sum + (c.estimated_value || 0), 0);
        const isWon = stage === 'won';
        const isLost = stage === 'lost';
        const isDragOver = dragOverStage === stage;

        return (
          <div
            key={stage}
            className="flex-shrink-0 w-72"
            onDragOver={(e) => handleDragOver(e, stage)}
            onDragLeave={handleDragLeave}
            onDrop={(e) => handleDrop(e, stage)}
          >
            <div
              className={cn(
                'rounded-xl border overflow-hidden transition-all duration-200',
                stageTopBorderColors[stage],
                'border-t-[3px]',
                isDragOver
                  ? 'border-blue-500/60 bg-blue-500/5 shadow-lg shadow-blue-500/10'
                  : 'border-slate-700 bg-slate-800/50',
                isWon && 'bg-gradient-to-b from-emerald-950/40 via-emerald-950/20 to-slate-800/50',
                isLost && 'bg-gradient-to-b from-red-950/30 via-red-950/10 to-slate-800/50'
              )}
            >
              {/* Column header */}
              <div className="px-4 py-3 border-b border-slate-700/80">
                <div className="flex items-center justify-between mb-1.5">
                  <div className="flex items-center gap-2">
                    <Badge variant={getStageColor(stage)}>
                      {stageLabels[stage] || stage}
                    </Badge>
                  </div>
                  <span className="text-xs font-medium text-slate-500 bg-slate-700/80 px-2 py-0.5 rounded-full">
                    {stageCards.length}
                  </span>
                </div>
                {totalValue > 0 && (
                  <p className="text-xs text-slate-500 font-medium">
                    {formatCurrency(totalValue)}
                  </p>
                )}
              </div>

              {/* Cards */}
              <div className="p-3 space-y-3 min-h-[200px] max-h-[calc(100vh-300px)] overflow-y-auto">
                {stageCards.map((card) => (
                  <div
                    key={card.id}
                    draggable
                    onDragStart={(e) => handleDragStart(e, card.id)}
                    onDragEnd={handleDragEnd}
                    onClick={() => onCardClick?.(card)}
                    className={cn(
                      'bg-slate-800 border border-slate-700 rounded-lg p-3 cursor-pointer hover:border-slate-500 hover:shadow-lg hover:shadow-black/20 transition-all duration-200 group',
                      draggingId === card.id && 'opacity-50 scale-95 rotate-1'
                    )}
                  >
                    {/* Top row: avatar + name + priority dot */}
                    <div className="flex items-center gap-2.5 mb-2.5">
                      <div
                        className={cn(
                          'w-8 h-8 rounded-full bg-gradient-to-br flex items-center justify-center flex-shrink-0 text-[10px] font-bold text-white shadow-inner',
                          getAvatarColor(card.company_name)
                        )}
                      >
                        {getCompanyInitials(card.company_name)}
                      </div>
                      <h4 className="text-sm font-semibold text-white group-hover:text-blue-400 transition-colors truncate flex-1">
                        {card.company_name}
                      </h4>
                      <div
                        className={cn('w-2.5 h-2.5 rounded-full flex-shrink-0 ring-2 ring-slate-800', priorityDotColors[card.priority])}
                        title={`${card.priority} priority`}
                      />
                    </div>

                    {/* Scheme type + units */}
                    <div className="flex items-center gap-2 mb-2">
                      <Badge variant={getSchemeTypeColor(card.scheme_type)} size="sm">
                        {card.scheme_type}
                      </Badge>
                      <span className="text-xs text-slate-500">{card.units} units</span>
                    </div>

                    {/* Estimated value */}
                    {card.estimated_value && card.estimated_value > 0 && (
                      <div className="mb-2">
                        <span className="text-xs font-semibold text-emerald-400">
                          {formatCurrency(card.estimated_value)}
                        </span>
                      </div>
                    )}

                    {/* Bottom row: council + BD score */}
                    <div className="flex items-center justify-between">
                      <span className="text-xs text-slate-500 truncate pr-2">{card.council}</span>
                      <div className="flex items-center gap-1 flex-shrink-0">
                        <span className="text-xs text-slate-500">BD:</span>
                        <span
                          className={cn(
                            'text-sm font-bold',
                            card.bd_score > 80
                              ? 'text-red-400'
                              : card.bd_score > 50
                              ? 'text-amber-400'
                              : 'text-green-400'
                          )}
                        >
                          {card.bd_score}
                        </span>
                      </div>
                    </div>

                    {/* Last activity */}
                    {card.last_activity && (
                      <div className="mt-2 pt-2 border-t border-slate-700/60">
                        <span className="text-[10px] text-slate-600">
                          {formatRelativeDate(card.last_activity)}
                        </span>
                      </div>
                    )}
                  </div>
                ))}
                {stageCards.length === 0 && (
                  <div className="text-center text-slate-600 text-xs py-8">
                    No opportunities
                  </div>
                )}
              </div>
            </div>
          </div>
        );
      })}
    </div>
  );
}
