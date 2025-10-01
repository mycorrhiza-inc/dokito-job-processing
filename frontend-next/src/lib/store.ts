import { create } from 'zustand';
import { Job, HealthResponse } from '@/types/api';

interface AppState {
  // Theme
  isDarkMode: boolean;
  toggleDarkMode: () => void;

  // Jobs
  jobs: Job[];
  selectedJob: Job | null;
  isJobsLoading: boolean;
  setJobs: (jobs: Job[]) => void;
  setSelectedJob: (job: Job | null) => void;
  setJobsLoading: (loading: boolean) => void;
  addJob: (job: Job) => void;
  updateJob: (id: string, updates: Partial<Job>) => void;
  removeJob: (id: string) => void;

  // Health status
  healthStatus: HealthResponse | null;
  setHealthStatus: (status: HealthResponse | null) => void;

  // UI state
  sidebarOpen: boolean;
  setSidebarOpen: (open: boolean) => void;
}

export const useAppStore = create<AppState>((set) => ({
  // Theme
  isDarkMode: false,
  toggleDarkMode: () => set((state) => ({ isDarkMode: !state.isDarkMode })),

  // Jobs
  jobs: [],
  selectedJob: null,
  isJobsLoading: false,
  setJobs: (jobs) => set({ jobs }),
  setSelectedJob: (selectedJob) => set({ selectedJob }),
  setJobsLoading: (isJobsLoading) => set({ isJobsLoading }),
  addJob: (job) => set((state) => ({ jobs: [job, ...state.jobs] })),
  updateJob: (id, updates) =>
    set((state) => ({
      jobs: state.jobs.map((job) =>
        job.id === id ? { ...job, ...updates } : job
      ),
      selectedJob: state.selectedJob?.id === id 
        ? { ...state.selectedJob, ...updates } 
        : state.selectedJob,
    })),
  removeJob: (id) =>
    set((state) => ({
      jobs: state.jobs.filter((job) => job.id !== id),
      selectedJob: state.selectedJob?.id === id ? null : state.selectedJob,
    })),

  // Health status
  healthStatus: null,
  setHealthStatus: (healthStatus) => set({ healthStatus }),

  // UI state
  sidebarOpen: false,
  setSidebarOpen: (sidebarOpen) => set({ sidebarOpen }),
}));