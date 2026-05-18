from hypergraph_scheduler.proposal.optimizer import *
from hypergraph_scheduler.proposal import optimizer as _proposal_optimizer


ARTIFACTS_DIR = _proposal_optimizer.ARTIFACTS_DIR


def build_scope_schedule_proposal(connection, scope, solver_backend=None):
	_proposal_optimizer.ARTIFACTS_DIR = ARTIFACTS_DIR
	return _proposal_optimizer.build_scope_schedule_proposal(connection, scope, solver_backend=solver_backend)


def build_recommendation_engine_schedule_proposal(connection, solver_backend=None):
	_proposal_optimizer.ARTIFACTS_DIR = ARTIFACTS_DIR
	return _proposal_optimizer.build_recommendation_engine_schedule_proposal(connection, solver_backend=solver_backend)
