import { describe, expect, it } from 'vitest';
import { accountAgeDays, decide, type UserSnapshot } from '../src/filter/rules.ts';

const DAY = 86_400;
const edit = Date.parse('2026-09-30T12:00:00Z') / 1000;
const account = (daysOld: number, editcount: number): UserSnapshot => ({
  registration: new Date((edit - daysOld * DAY) * 1000).toISOString(),
  editcount,
});
const registered = (user: UserSnapshot | undefined, policy: 'bots' | 'autoconfirmed' | 'extendedconfirmed') =>
  decide({ userClass: 'registered', timestamp: edit }, user, policy);

describe('free filter', () => {
  it('drops bots under every policy', () => {
    for (const p of ['bots', 'autoconfirmed', 'extendedconfirmed'] as const) {
      expect(decide({ userClass: 'bot', timestamp: edit }, undefined, p)).toEqual({ keep: false, rule: 'bot' });
    }
  });

  it('the bots policy keeps every human edit, however trusted', () => {
    expect(registered(account(5000, 1_000_000), 'bots').keep).toBe(true);
  });

  it('applies the autoconfirmed threshold: 4 days and 10 edits, both required', () => {
    expect(registered(account(4, 10), 'autoconfirmed').keep).toBe(false);
    expect(registered(account(3.9, 10_000), 'autoconfirmed').keep).toBe(true);
    expect(registered(account(400, 9), 'autoconfirmed').keep).toBe(true);
  });

  it('applies the extended-confirmed threshold: 30 days and 500 edits, both required', () => {
    expect(registered(account(30, 500), 'extendedconfirmed')).toEqual({ keep: false, rule: 'trusted-account' });
    expect(registered(account(29.9, 50_000), 'extendedconfirmed').keep).toBe(true);
    expect(registered(account(3000, 499), 'extendedconfirmed').keep).toBe(true);
  });

  it('measures account age at the time of the edit, not now', () => {
    expect(accountAgeDays(account(10, 0), edit)).toBeCloseTo(10);
    expect(accountAgeDays(account(10, 0), edit - 5 * DAY)).toBeCloseTo(5);
  });

  it('treats accounts older than registration records as old', () => {
    expect(registered({ registration: null, editcount: 600 }, 'extendedconfirmed').keep).toBe(false);
  });

  it('keeps temporary accounts and unknown accounts, whatever their numbers', () => {
    expect(decide({ userClass: 'temporary', timestamp: edit }, account(5000, 1_000_000), 'autoconfirmed').keep).toBe(true);
    expect(registered(undefined, 'autoconfirmed').keep).toBe(true);
  });
});
