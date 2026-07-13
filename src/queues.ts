export type RecordingRole = 'client' | 'trainer' | 'coach';
export type JoinAsMode = RecordingRole | 'all';

export const RECORDING_ROLES: readonly RecordingRole[] = [
  'client',
  'trainer',
  'coach',
];

export function classRecordingStartQueue(role: RecordingRole): string {
  return `class-recording-start-${role}`;
}

export function classRecordingStopQueue(role: RecordingRole): string {
  return `class-recording-stop-${role}`;
}
