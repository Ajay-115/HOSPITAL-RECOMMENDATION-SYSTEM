-- Real-Time Hospital Resource, Surgery and Blood Donation Recommendation System
-- SQLite Relational Database Schema (Phase 5 Persistent Storage)

PRAGMA foreign_keys = ON;

-- 1. HOSPITALS MASTER TABLE
CREATE TABLE IF NOT EXISTS hospitals (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    specialty TEXT NOT NULL,
    district TEXT NOT NULL,
    location TEXT DEFAULT '',
    latitude REAL NOT NULL DEFAULT 12.9716,
    longitude REAL NOT NULL DEFAULT 77.5946,
    rating REAL DEFAULT 4.5,
    total_beds INTEGER NOT NULL DEFAULT 100,
    available_beds INTEGER NOT NULL DEFAULT 20,
    icu_total INTEGER NOT NULL DEFAULT 20,
    icu_available INTEGER NOT NULL DEFAULT 5,
    emergency_services TEXT DEFAULT 'Yes',
    emergency_load TEXT DEFAULT 'Medium',
    standard_cost INTEGER DEFAULT 35000,
    average_wait_minutes INTEGER DEFAULT 30,
    queue_length INTEGER DEFAULT 10,
    phone TEXT DEFAULT '+91-80-2800-0000',
    address TEXT DEFAULT '',
    status TEXT DEFAULT 'Active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_hospitals_specialty ON hospitals(specialty);
CREATE INDEX IF NOT EXISTS idx_hospitals_district ON hospitals(district);
CREATE INDEX IF NOT EXISTS idx_hospitals_location ON hospitals(location);
CREATE INDEX IF NOT EXISTS idx_hospitals_status ON hospitals(status);

-- 2. SURGERIES TABLE
CREATE TABLE IF NOT EXISTS surgeries (
    id TEXT PRIMARY KEY,
    hospital_id TEXT NOT NULL,
    surgery_name TEXT NOT NULL,
    specialty TEXT NOT NULL,
    estimated_cost INTEGER NOT NULL,
    average_wait_minutes INTEGER DEFAULT 30,
    waiting_days INTEGER DEFAULT 3,
    available_slots INTEGER DEFAULT 4,
    location TEXT DEFAULT '',
    availability TEXT DEFAULT 'Available',
    status TEXT DEFAULT 'Active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (hospital_id) REFERENCES hospitals(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_surgeries_hospital ON surgeries(hospital_id);
CREATE INDEX IF NOT EXISTS idx_surgeries_specialty ON surgeries(specialty);

-- 3. BLOOD BANKS TABLE
CREATE TABLE IF NOT EXISTS blood_banks (
    id TEXT PRIMARY KEY,
    hospital_id TEXT,
    name TEXT,
    blood_group TEXT NOT NULL,
    units_available INTEGER NOT NULL DEFAULT 0,
    district TEXT NOT NULL,
    contact TEXT,
    last_updated TEXT,
    status TEXT DEFAULT 'Active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (hospital_id) REFERENCES hospitals(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_blood_banks_group ON blood_banks(blood_group);
CREATE INDEX IF NOT EXISTS idx_blood_banks_district ON blood_banks(district);

-- 4. RESOURCES TABLE
CREATE TABLE IF NOT EXISTS resources (
    id TEXT PRIMARY KEY,
    hospital_id TEXT NOT NULL,
    resource_name TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    total_quantity INTEGER NOT NULL,
    available_quantity INTEGER NOT NULL,
    status TEXT DEFAULT 'Available',
    last_updated TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (hospital_id) REFERENCES hospitals(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_resources_hospital ON resources(hospital_id);
CREATE INDEX IF NOT EXISTS idx_resources_type ON resources(resource_type);
