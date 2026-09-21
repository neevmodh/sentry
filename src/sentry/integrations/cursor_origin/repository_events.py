from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from typing import Any

from sentry.constants import ObjectStatus
from sentry.integrations.cursor_origin.constants import CURSOR_ORIGIN_WEB_BASE_URL
from sentry.integrations.cursor_origin.handlers import WebhookEventHandler
from sentry.integrations.cursor_origin.repository import active_repositories
from sentry.integrations.cursor_origin.webhook_types import (
    RepositoryDeletedEvent,
    RepositoryMetadataEvent,
    RepositorySnapshot,
)
from sentry.integrations.services.integration.model import (
    RpcIntegration,
    RpcOrganizationIntegration,
)
from sentry.integrations.services.repository import repository_service
from sentry.integrations.source_code_management.repo_audit import log_repo_change
from sentry.integrations.source_code_management.sync_repos import DISABLE_ACTIVITY_CUTOFF_DAYS
from sentry.integrations.types import IntegrationProviderSlug
from sentry.integrations.utils.metrics import IntegrationWebhookEventType
from sentry.models.organization import Organization
from sentry.models.repository import Repository
from sentry.organizations.services.organization.serial import serialize_rpc_organization
from sentry.plugins.providers.integration_repository import get_integration_repository_provider
from sentry.utils import metrics

logger = logging.getLogger("sentry.integrations.cursor_origin")

PROVIDER = f"integrations:{IntegrationProviderSlug.CURSOR_ORIGIN.value}"


class RepositoryMetadataUpdatedHandler(WebhookEventHandler):
    """Apply a rename or a default-branch change."""

    EVENT_TYPE = IntegrationWebhookEventType.INBOUND_SYNC

    def __call__(
        self,
        payload: Mapping[str, Any],
        delivery_id: str,
        integration: RpcIntegration,
        org_integrations: Sequence[RpcOrganizationIntegration],
    ) -> None:
        snapshot = RepositoryMetadataEvent.from_payload(payload).repository
        for repo in active_repositories(snapshot.id, org_integrations):
            reconcile_repository(repo, snapshot, delivery_id)


def reconcile_repository(repo: Repository, snapshot: RepositorySnapshot, delivery_id: str) -> None:
    """Bring one row back in step with Origin."""
    url = f"{CURSOR_ORIGIN_WEB_BASE_URL}/{snapshot.full_name}"
    config = {
        **repo.config,
        "name": snapshot.full_name,
        "default_branch": snapshot.default_branch,
    }

    if repo.name == snapshot.full_name and repo.url == url and repo.config == config:
        return

    logger.info(
        "cursor_origin.repository.reconciled",
        extra={
            "delivery_id": delivery_id,
            "repository_id": repo.id,
            "previous_name": repo.name,
            "new_name": snapshot.full_name,
            "previous_default_branch": repo.config.get("default_branch"),
            "new_default_branch": snapshot.default_branch,
        },
    )
    repo.update(name=snapshot.full_name, url=url, config=config)


class RepositoryCreatedHandler(WebhookEventHandler):
    EVENT_TYPE = IntegrationWebhookEventType.INBOUND_SYNC

    def __call__(
        self,
        payload: Mapping[str, Any],
        delivery_id: str,
        integration: RpcIntegration,
        org_integrations: Sequence[RpcOrganizationIntegration],
    ) -> None:
        snapshot = RepositoryMetadataEvent.from_payload(payload).repository
        provider = get_integration_repository_provider(integration)
        config = {
            "name": snapshot.full_name,
            "external_id": snapshot.id,
            "default_branch": snapshot.default_branch,
            "integration_id": integration.id,
        }

        for organization in Organization.objects.filter(
            id__in=[oi.organization_id for oi in org_integrations]
        ):
            created, reactivated, _ = provider.create_repositories(
                configs=[config], organization=serialize_rpc_organization(organization)
            )
            if created:
                repository_service.auto_link_repos_by_name(
                    organization_id=organization.id, repo_ids=[repo.id for repo in created]
                )
            for repo in created:
                log_repo_change(
                    event_name="REPO_ADDED",
                    organization_id=organization.id,
                    repo=repo,
                    source="Cursor Origin webhook",
                    provider=integration.provider,
                )
            for repo in reactivated:
                log_repo_change(
                    event_name="REPO_ENABLED",
                    organization_id=organization.id,
                    repo=repo,
                    source="Cursor Origin webhook",
                    provider=integration.provider,
                )


class RepositoryDeletedHandler(WebhookEventHandler):
    EVENT_TYPE = IntegrationWebhookEventType.INBOUND_SYNC

    def __call__(
        self,
        payload: Mapping[str, Any],
        delivery_id: str,
        integration: RpcIntegration,
        org_integrations: Sequence[RpcOrganizationIntegration],
    ) -> None:
        external_id = RepositoryDeletedEvent.from_payload(payload).repository.id
        for org_integration in org_integrations:
            organization_id = org_integration.organization_id
            if repository_service.find_recently_active_repo_external_ids(
                organization_id=organization_id,
                integration_id=integration.id,
                provider=PROVIDER,
                external_ids=[external_id],
                cutoff_days=DISABLE_ACTIVITY_CUTOFF_DAYS,
            ):
                logger.info(
                    "cursor_origin.repository.disable_skipped_due_to_activity",
                    extra={
                        "delivery_id": delivery_id,
                        "organization_id": organization_id,
                        "external_id": external_id,
                        "cutoff_days": DISABLE_ACTIVITY_CUTOFF_DAYS,
                    },
                )
                metrics.incr(
                    "cursor_origin.repository.disable_skipped_due_to_activity", sample_rate=1.0
                )
                continue

            active = [
                repo
                for repo in repository_service.get_repositories(
                    organization_id=organization_id,
                    integration_id=integration.id,
                    providers=[PROVIDER],
                    external_id=external_id,
                )
                if repo.status == ObjectStatus.ACTIVE
            ]
            repository_service.disable_repositories_by_external_ids(
                organization_id=organization_id,
                integration_id=integration.id,
                provider=PROVIDER,
                external_ids=[external_id],
            )
            for repo in active:
                log_repo_change(
                    event_name="REPO_DISABLED",
                    organization_id=organization_id,
                    repo=repo,
                    source="Cursor Origin webhook",
                    provider=integration.provider,
                )
