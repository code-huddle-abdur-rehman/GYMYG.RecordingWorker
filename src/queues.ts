export type RecordingRole = 'client' | 'trainer' | 'coach';

export function classRecordingStartQueue(role: RecordingRole): string {
  return `class-recording-start-${role}`;
}

export function classRecordingStopQueue(role: RecordingRole): string {
  return `class-recording-stop-${role}`;
}
