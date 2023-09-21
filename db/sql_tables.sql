-- table definitions for the hubspot integration schema


CREATE TABLE hubspot_integration.contacts (
    "hubspotID" bigint PRIMARY KEY,
    email character varying NOT NULL,
    created timestamp without time zone NOT NULL,
    updated timestamp without time zone
);


CREATE TABLE hubspot_integration.memberships (
    "hubspotID" bigint PRIMARY KEY,
    "membershipID" bigint NOT NULL,
    created timestamp without time zone NOT NULL,
    updated timestamp without time zone
);



CREATE TABLE hubspot_integration.serials (
    "hubspotID" bigint PRIMARY KEY,
    serial character varying NOT NULL,
    created timestamp without time zone NOT NULL,
    updated timestamp without time zone
);


CREATE TABLE hubspot_integration.workspaces (
    "hubspotID" bigint PRIMARY KEY,
    "workspaceID" bigint NOT NULL,
    created timestamp without time zone NOT NULL,
    updated timestamp without time zone
);