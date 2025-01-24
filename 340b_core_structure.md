# 340B OPAIS Core Data Structure

## Overview

- File: OPA_CE_DAILY_PUBLIC.JSON
- Sample Size: 4 records

## Core Structure

The file contains a root object with a 'coveredEntities' array containing individual entity records.

### Primary Fields


#### coveredEntities

| Field | Data Type | Sample Values |
|-------|-----------|---------------|
| coveredEntities.billingAddress.addressLine1 | str | 276 Fieldstone Drive |
| coveredEntities.billingAddress.city | str | Jonesville |
| coveredEntities.billingAddress.organization | str | STONE MOUNTAIN HEALTH SERVICES |
| coveredEntities.billingAddress.state | str | VA |
| coveredEntities.billingAddress.zip | str | 24263 |
| coveredEntities.ceId | int | 7201 |
| coveredEntities.entityType | str | BL |
| coveredEntities.grantNumber | str | H37RH00048 |
| coveredEntities.id340B | str | BL030740 |
| coveredEntities.medicaidNumber | str | Q047757 |
| coveredEntities.medicaidNumbers.medicaidNumber | str | 64036072 |
| coveredEntities.medicaidNumbers.state | str | KY |
| coveredEntities.name | str | STONE MOUNTAIN HEALTH SERVICES |
| coveredEntities.participating | str | TRUE |
| coveredEntities.participatingStartDate | str | 1999-10-01T00:00:00 |
| coveredEntities.shippingAddresses.addressLine1 | str | 213 Monarch Road |
| coveredEntities.shippingAddresses.city | str | ST CHARLES |
| coveredEntities.shippingAddresses.is340BStreetAddress | bool | True |
| coveredEntities.shippingAddresses.state | str | VA |
| coveredEntities.shippingAddresses.zip | str | 24282 |
| coveredEntities.state | str | TN |
| coveredEntities.streetAddress.addressLine1 | str | 213 Monarch Road |
| coveredEntities.streetAddress.city | str | ST CHARLES |
| coveredEntities.streetAddress.state | str | VA |
| coveredEntities.streetAddress.zip | str | 24282 |
| coveredEntities.subName | str | ST CHARLES HEALTH COUNCIL |

#### npiNumbers

| Field | Data Type | Sample Values |
|-------|-----------|---------------|
| npiNumbers.npiNumber | str | 1215284153 |
| npiNumbers.state | str | VA |

#### Core Fields

| Field | Data Type | Sample Values |
|-------|-----------|---------------|
| addressLine1 | str | 226 Medical Plaza Lane |
| beginDate | str | 2015-07-01T00:00:00 |
| ceId | int | 17616 |
| certifiedDecertifiedDate | str | 2024-02-16T09:29:10.8756857 |
| city | str | Whitesburg |
| contractId | int | 20059 |
| editDate | str | 2025-01-17T11:04:58.4482606 |
| entityType | str | BL |
| grantNumber | str | H37RH00048 |
| id340B | str | BL03074A |
| is340BStreetAddress | bool | False |
| medicaidNumber | str | 7100958890 |
| name | str | STONE MOUNTAIN HEALTH SERVICES |
| npiNumber | str | 1740252501 |
| organization | str | Mountain Comprehensive Health Corporation |
| participating | str | FALSE |
| participatingStartDate | str | 2008-07-01T00:00:00 |
| pharmacyId | int | 5043 |
| phoneNumber | str | 6065930382 |
| state | str | KY |
| subName | str | ST CHARLES HEALTH COUNCIL |
| terminationDate | str | 2016-04-01T00:00:00 |
| terminationReason | str | Site closure |
| zip | str | 41858 |

#### authorizingOfficial

| Field | Data Type | Sample Values |
|-------|-----------|---------------|
| authorizingOfficial.name | str | MALCOLM  PERDUE |
| authorizingOfficial.phoneNumber | str | 2765465310 |
| authorizingOfficial.title | str | CEO |

#### primaryContact

| Field | Data Type | Sample Values |
|-------|-----------|---------------|
| primaryContact.name | str | Saundra  Jones |
| primaryContact.phoneNumber | str | 2765465310 |
| primaryContact.phoneNumberExtension | str | 3102 |
| primaryContact.title | str | COO |

#### contractPharmacies

| Field | Data Type | Sample Values |
|-------|-----------|---------------|
| contractPharmacies.address.addressLine1 | str | 99 KY 11 SOUTH |
| contractPharmacies.address.city | str | BOONEVILLE |
| contractPharmacies.address.state | str | KY |
| contractPharmacies.address.zip | str | 41314 |
| contractPharmacies.beginDate | str | 2015-07-01T00:00:00 |
| contractPharmacies.certifiedDecertifiedDate | str | 2024-01-31T09:14:36.6628594 |
| contractPharmacies.comments | str |  6/3/2008 UPDATED BILLING |
| contractPharmacies.contractId | int | 20058 |
| contractPharmacies.editDate | str | 2024-01-31T09:14:36.6628594 |
| contractPharmacies.name | str | B & H APOTHECARY |
| contractPharmacies.pharmacyId | int | 92368 |
| contractPharmacies.phoneNumber | str | 8598931064 |

#### streetAddress

| Field | Data Type | Sample Values |
|-------|-----------|---------------|
| streetAddress.addressLine1 | str | 185 Redwood Avenue Ste 102 |
| streetAddress.city | str | PENNINGTON GAP |
| streetAddress.state | str | VA |
| streetAddress.zip | str | 24277 |

#### billingAddress

| Field | Data Type | Sample Values |
|-------|-----------|---------------|
| billingAddress.addressLine1 | str | 185 Redwood Avenue Ste 102 |
| billingAddress.addressLine2 | str | 226 Medical Plaza Lane |
| billingAddress.city | str | PENNINGTON GAP |
| billingAddress.organization | str | MOUNTAIN COMPREHENSIVE HEALTH CORP. |
| billingAddress.state | str | VA |
| billingAddress.zip | str | 24277 |

#### shippingAddresses

| Field | Data Type | Sample Values |
|-------|-----------|---------------|
| shippingAddresses.addressLine1 | str | 185 Redwood Avenue Ste 102 |
| shippingAddresses.city | str | PENNINGTON GAP |
| shippingAddresses.is340BStreetAddress | bool | True |
| shippingAddresses.state | str | VA |
| shippingAddresses.zip | str | 24277 |

#### medicaidNumbers

| Field | Data Type | Sample Values |
|-------|-----------|---------------|
| medicaidNumbers.authorizingOfficial.name | str | MALCOLM  PERDUE |
| medicaidNumbers.authorizingOfficial.phoneNumber | str | 2765465310 |
| medicaidNumbers.authorizingOfficial.title | str | CEO |
| medicaidNumbers.billingAddress.addressLine1 | str | 185 Redwood Avenue |
| medicaidNumbers.billingAddress.addressLine2 | str | SUITE 102 |
| medicaidNumbers.billingAddress.city | str | PENNINGTON GAP |
| medicaidNumbers.billingAddress.organization | str | STONE MOUNTAIN HEALTH SERVICES |
| medicaidNumbers.billingAddress.state | str | VA |
| medicaidNumbers.billingAddress.zip | str | 24277 |
| medicaidNumbers.ceId | int | 17683 |
| medicaidNumbers.contractPharmacies.certifiedDecertifiedDate | str | 2016-03-09T00:00:00 |
| medicaidNumbers.contractPharmacies.editDate | str | 2016-03-09T11:51:14.757 |
| medicaidNumbers.entityType | str | BL |
| medicaidNumbers.grantNumber | str | H37RH00048 |
| medicaidNumbers.id340B | str | BL03074B |
| medicaidNumbers.medicaidNumber | str | Q047757 |
| medicaidNumbers.medicaidNumbers.ceId | int | 51875 |
| medicaidNumbers.medicaidNumbers.entityType | str | BL |
| medicaidNumbers.medicaidNumbers.grantNumber | str | H37RH00048 |
| medicaidNumbers.medicaidNumbers.id340B | str | BL03074C |
| medicaidNumbers.medicaidNumbers.name | str | Stone Mountain Health Services |
| medicaidNumbers.medicaidNumbers.participating | str | TRUE |
| medicaidNumbers.medicaidNumbers.participatingStartDate | str | 2017-01-01T00:00:00 |
| medicaidNumbers.medicaidNumbers.subName | str | St. Charles Health Council, Inc, Vansant Respiratory Care Center |
| medicaidNumbers.name | str | STONE MOUNTAIN HEALTH SERVICES |
| medicaidNumbers.npiNumbers.ceId | int | 12236 |
| medicaidNumbers.npiNumbers.entityType | str | BL |
| medicaidNumbers.npiNumbers.grantNumber | str | H37RH00050 |
| medicaidNumbers.npiNumbers.id340B | str | BL040600 |
| medicaidNumbers.npiNumbers.name | str | MOUNTAIN COMPREHENSIVE HEALTH CORP. |
| medicaidNumbers.npiNumbers.npiNumber | str | 1740252501 |
| medicaidNumbers.npiNumbers.participating | str | TRUE |
| medicaidNumbers.npiNumbers.participatingStartDate | str | 2004-10-01T00:00:00 |
| medicaidNumbers.npiNumbers.subName | str | WHITESBURG MEDICAL CLINIC |
| medicaidNumbers.participating | str | FALSE |
| medicaidNumbers.participatingStartDate | str | 2008-07-01T00:00:00 |
| medicaidNumbers.primaryContact.name | str | KAY G. MATLOCK |
| medicaidNumbers.primaryContact.phoneNumber | str | 2765465310 |
| medicaidNumbers.primaryContact.title | str | PROGRAM DEVELOPMENT OFFICER |
| medicaidNumbers.shippingAddresses.addressLine1 | str | 10953 Riverside Drive |
| medicaidNumbers.shippingAddresses.city | str | OAKWOOD |
| medicaidNumbers.shippingAddresses.is340BStreetAddress | bool | True |
| medicaidNumbers.shippingAddresses.state | str | VA |
| medicaidNumbers.shippingAddresses.zip | str | 24631 |
| medicaidNumbers.shippingAddresses.zip4 | str | 1005 |
| medicaidNumbers.state | str | TN |
| medicaidNumbers.streetAddress.addressLine1 | str | 10953 Riverside Drive |
| medicaidNumbers.streetAddress.city | str | OAKWOOD |
| medicaidNumbers.streetAddress.state | str | VA |
| medicaidNumbers.streetAddress.zip | str | 24631 |
| medicaidNumbers.streetAddress.zip4 | str | 1005 |
| medicaidNumbers.subName | str | OAKWOOD RESPIRATORY CARE CENTER |
| medicaidNumbers.terminationDate | str | 2016-04-01T00:00:00 |
| medicaidNumbers.terminationReason | str | Site closure |

#### address

| Field | Data Type | Sample Values |
|-------|-----------|---------------|
| address.addressLine1 | str | 478 KY 11 N |
| address.addressLine2 | str | 132 VILLAGE CENTER RD |
| address.city | str | BOONEVILLE |
| address.state | str | KY |
| address.zip | str | 41314 |
| address.zip4 | str | 9155 |

## Suggested Database Structure

Based on the analysis, the following table structure is recommended:


### Main Table

```sql
CREATE TABLE covered_entities (
    id SERIAL PRIMARY KEY,
    id340b TEXT,
    name TEXT,
    entity_type TEXT,
    participating BOOLEAN,
    participating_start_date TIMESTAMP,
    grant_number TEXT
);
```

### Related Tables

```sql
CREATE TABLE medicaid_numbers (
    id SERIAL PRIMARY KEY,
    covered_entity_id INTEGER REFERENCES covered_entities(id),
    medicaid_number TEXT,
    state TEXT
);

CREATE TABLE npi_numbers (
    id SERIAL PRIMARY KEY,
    covered_entity_id INTEGER REFERENCES covered_entities(id),
    npi_number TEXT,
    state TEXT
);

CREATE TABLE addresses (
    id SERIAL PRIMARY KEY,
    covered_entity_id INTEGER REFERENCES covered_entities(id),
    address_type TEXT,  -- billing, shipping, etc
    address_line1 TEXT,
    address_line2 TEXT,
    city TEXT,
    state TEXT,
    zip TEXT
);```
