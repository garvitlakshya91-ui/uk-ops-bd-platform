import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import * as api from '@/lib/api';
import type {
  ApplicationParams,
  PipelineParams,
  AlertParams,
  PaginationParams,
} from '@/lib/api';

// Application hooks
export function useApplications(params?: ApplicationParams) {
  return useQuery({
    queryKey: ['applications', params],
    queryFn: () => api.getApplications(params),
  });
}

export function useApplication(id: string) {
  return useQuery({
    queryKey: ['application', id],
    queryFn: () => api.getApplication(id),
    enabled: !!id,
  });
}

export function useApplicationStats() {
  return useQuery({
    queryKey: ['applicationStats'],
    queryFn: api.getApplicationStats,
  });
}

// Company hooks
export function useCompanies(params?: PaginationParams) {
  return useQuery({
    queryKey: ['companies', params],
    queryFn: () => api.getCompanies(params),
  });
}

export function useCompany(id: string) {
  return useQuery({
    queryKey: ['company', id],
    queryFn: () => api.getCompany(id),
    enabled: !!id,
  });
}

export function useMergeCompanies() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ primaryId, duplicateIds }: { primaryId: string; duplicateIds: string[] }) =>
      api.mergeCompanies(primaryId, duplicateIds),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['companies'] });
    },
  });
}

// Pipeline hooks
export function usePipelineOpportunities(params?: PipelineParams) {
  return useQuery({
    queryKey: ['pipeline', params],
    queryFn: () => api.getPipelineOpportunities(params),
  });
}

export function useUpdateOpportunityStage() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ id, stage }: { id: string; stage: string }) =>
      api.updateOpportunityStage(id, stage),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['pipeline'] });
    },
  });
}

export function usePipelineStats() {
  return useQuery({
    queryKey: ['pipelineStats'],
    queryFn: api.getPipelineStats,
  });
}

// Scheme hooks
export function useSchemes(params?: PaginationParams) {
  return useQuery({
    queryKey: ['schemes', params],
    queryFn: () => api.getSchemes(params),
  });
}

export function useScheme(id: string) {
  return useQuery({
    queryKey: ['scheme', id],
    queryFn: () => api.getScheme(id),
    enabled: !!id,
  });
}

// Dashboard hooks
export function useDashboardStats() {
  return useQuery({
    queryKey: ['dashboardStats'],
    queryFn: api.getDashboardStats,
  });
}

export function useTrendData(days: number = 30) {
  return useQuery({
    queryKey: ['trendData', days],
    queryFn: () => api.getTrendData(days),
  });
}

export function useTopOpportunities() {
  return useQuery({
    queryKey: ['topOpportunities'],
    queryFn: api.getTopOpportunities,
  });
}

// Alert hooks
export function useAlerts(params?: AlertParams) {
  return useQuery({
    queryKey: ['alerts', params],
    queryFn: () => api.getAlerts(params),
  });
}

export function useMarkAlertRead() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => api.markAlertRead(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['alerts'] });
    },
  });
}

// Scraper hooks
export function useScraperHealth() {
  return useQuery({
    queryKey: ['scraperHealth'],
    queryFn: api.getScraperHealth,
  });
}

export function useTriggerScrape() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (councilId: string) => api.triggerScrape(councilId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['scraperHealth'] });
    },
  });
}

export function useScraperHistory(councilId: string) {
  return useQuery({
    queryKey: ['scraperHistory', councilId],
    queryFn: () => api.getScraperHistory(councilId),
    enabled: !!councilId,
  });
}
