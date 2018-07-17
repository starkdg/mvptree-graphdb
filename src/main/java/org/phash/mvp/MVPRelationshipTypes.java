package org.phash.mvp;

import org.neo4j.graphdb.RelationshipType;

enum MVPRelationshipTypes implements RelationshipType {
	TO_VP, TO_CHILD, TO_DP
}
