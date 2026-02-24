-- Migration 013: Add local storage provider enum value

ALTER TYPE storage_provider ADD VALUE IF NOT EXISTS 'local';
