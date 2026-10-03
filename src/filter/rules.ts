// Phase 3: the free filter. Cheap, explainable rules that drop edits before any model sees them.
//
// Pure: the same event and the same user snapshot always give the same decision, so replaying an
// offset range reproduces the filter's output exactly. User facts come from a snapshot taken
// once and stored, never from a live lookup at decision time.
//
// The thresholds are Wikipedia's own trust levels, not values fitted to data (DECISIONS.md D10).

import type { UserClass } from '../phase0/events.ts';

/** What the Action API says about an account, frozen at `asOf`. */
export interface UserSnapshot {
  /** ISO time the account was created; null for accounts older than the field (pre-2006). */
  registration: string | null;
  /** Edit count when the snapshot was taken (not at the time of the edit). */
  editcount: number;
}

/** The parts of a recentchange event the filter reads (userClass from `classifyUser`). */
export interface FilterInput {
  userClass: UserClass;
  /** Unix seconds, the edit's own time. */
  timestamp: number;
}

export type Policy = 'bots' | 'autoconfirmed' | 'extendedconfirmed';
export const POLICIES: readonly Policy[] = ['bots', 'autoconfirmed', 'extendedconfirmed'];

/** Default for the system, fixed before evaluation (D10). */
export const DEFAULT_POLICY: Policy = 'extendedconfirmed';

/** Wikipedia's thresholds: autoconfirmed = 4 days and 10 edits; extended confirmed = 30 days and 500 edits. */
const TRUST: Record<Exclude<Policy, 'bots'>, { days: number; edits: number }> = {
  autoconfirmed: { days: 4, edits: 10 },
  extendedconfirmed: { days: 30, edits: 500 },
};

export type Decision =
  | { keep: true; rule: 'kept' }
  | { keep: false; rule: 'bot' | 'trusted-account' };

const DAY_S = 86_400;

/** Account age in days at the moment of the edit; Infinity for accounts that predate registration dates. */
export function accountAgeDays(user: UserSnapshot, editTimestamp: number): number {
  if (user.registration === null) return Infinity;
  return (editTimestamp - Date.parse(user.registration) / 1000) / DAY_S;
}

export function decide(edit: FilterInput, user: UserSnapshot | undefined, policy: Policy): Decision {
  if (edit.userClass === 'bot') return { keep: false, rule: 'bot' };
  if (policy === 'bots') return { keep: true, rule: 'kept' };
  // Temporary accounts can never reach either trust level, whatever their numbers say.
  if (edit.userClass !== 'registered' || !user) {
    return { keep: true, rule: 'kept' };
  }
  const t = TRUST[policy];
  if (accountAgeDays(user, edit.timestamp) >= t.days && user.editcount >= t.edits) {
    return { keep: false, rule: 'trusted-account' };
  }
  return { keep: true, rule: 'kept' };
}
