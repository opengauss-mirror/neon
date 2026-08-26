use anyhow::{Context, Result};
use async_trait::async_trait;
use pageserver_api::controller_api::{
    NodeConfigureRequest, NodeDescribeResponse, NodeRegisterRequest, SafekeeperDescribeResponse,
    SafekeeperSchedulingPolicyRequest, ShardsPreferredAzsRequest, ShardsPreferredAzsResponse,
    TenantCreateRequest, TenantCreateResponse, TenantDescribeResponse, TenantLocateResponse,
    TenantPolicyRequest, TenantShardMigrateRequest, TenantShardMigrateResponse,
    TenantTimelineDescribeResponse, TimelineSafekeeperMigrateRequest,
};
use pageserver_api::models::{
    TenantConfigRequest, TenantShardSplitRequest, TenantShardSplitResponse, TimelineCreateRequest,
    TimelineCreateResponseStorcon, TimelineInfo,
};
use pageserver_api::shard::TenantShardId;
use pageserver_client::mgmt_api::ResponseErrorMessageExt;
use reqwest::Method;
use serde::Serialize;
use serde::de::DeserializeOwned;
use url::Url;
use utils::id::NodeId;
use utils::id::{TenantId, TimelineId};

use crate::storage_controller::StorageController;

#[async_trait]
pub trait StorageControllerApi: Sync {
    async fn tenant_create(&self, req: TenantCreateRequest) -> Result<TenantCreateResponse>;

    async fn tenant_import(&self, tenant_id: TenantId) -> Result<TenantCreateResponse>;

    async fn tenant_locate(&self, tenant_id: TenantId) -> Result<TenantLocateResponse>;

    async fn tenant_list(&self, limit: Option<usize>) -> Result<Vec<TenantDescribeResponse>>;

    async fn tenant_describe(&self, tenant_id: TenantId) -> Result<TenantDescribeResponse>;

    async fn tenant_delete(&self, tenant_id: TenantId) -> Result<()>;

    async fn tenant_policy(&self, tenant_id: TenantId, req: TenantPolicyRequest) -> Result<()>;

    async fn tenant_shard_split(
        &self,
        tenant_id: TenantId,
        req: TenantShardSplitRequest,
    ) -> Result<TenantShardSplitResponse>;

    async fn tenant_shard_migrate(
        &self,
        tenant_shard_id: TenantShardId,
        req: TenantShardMigrateRequest,
    ) -> Result<TenantShardMigrateResponse>;

    async fn tenant_shard_migrate_secondary(
        &self,
        tenant_shard_id: TenantShardId,
        req: TenantShardMigrateRequest,
    ) -> Result<TenantShardMigrateResponse>;

    async fn tenant_shard_cancel_reconcile(&self, tenant_shard_id: TenantShardId) -> Result<()>;

    async fn update_preferred_azs(
        &self,
        req: ShardsPreferredAzsRequest,
    ) -> Result<ShardsPreferredAzsResponse>;

    async fn tenant_timeline_create(
        &self,
        tenant_id: TenantId,
        req: TimelineCreateRequest,
    ) -> Result<TimelineCreateResponseStorcon>;

    async fn tenant_timeline_detail(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> Result<TimelineInfo>;

    async fn set_tenant_config(&self, req: &TenantConfigRequest) -> Result<()>;

    async fn node_register(&self, req: NodeRegisterRequest) -> Result<()>;

    async fn node_configure(&self, req: NodeConfigureRequest) -> Result<()>;

    async fn node_list(&self) -> Result<Vec<NodeDescribeResponse>>;

    async fn node_drain(&self, node_id: NodeId) -> Result<()>;

    async fn node_cancel_drain(&self, node_id: NodeId) -> Result<()>;

    async fn node_fill(&self, node_id: NodeId) -> Result<()>;

    async fn node_cancel_fill(&self, node_id: NodeId) -> Result<()>;

    async fn node_start_delete(&self, node_id: NodeId, force: bool) -> Result<()>;

    async fn safekeeper_list(&self) -> Result<Vec<SafekeeperDescribeResponse>>;

    async fn safekeeper_scheduling(
        &self,
        node_id: NodeId,
        req: SafekeeperSchedulingPolicyRequest,
    ) -> Result<()>;

    async fn timeline_safekeeper_migrate(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        req: TimelineSafekeeperMigrateRequest,
    ) -> Result<()>;
}

#[async_trait]
impl StorageControllerApi for StorageController {
    async fn tenant_create(&self, req: TenantCreateRequest) -> Result<TenantCreateResponse> {
        StorageController::tenant_create(self, req).await
    }

    async fn tenant_import(&self, tenant_id: TenantId) -> Result<TenantCreateResponse> {
        StorageController::tenant_import(self, tenant_id).await
    }

    async fn tenant_locate(&self, tenant_id: TenantId) -> Result<TenantLocateResponse> {
        StorageController::tenant_locate(self, tenant_id).await
    }

    async fn tenant_list(&self, limit: Option<usize>) -> Result<Vec<TenantDescribeResponse>> {
        StorageController::tenant_list(self, limit).await
    }

    async fn tenant_describe(&self, tenant_id: TenantId) -> Result<TenantDescribeResponse> {
        StorageController::tenant_describe(self, tenant_id).await
    }

    async fn tenant_delete(&self, tenant_id: TenantId) -> Result<()> {
        StorageController::tenant_delete(self, tenant_id).await
    }

    async fn tenant_policy(&self, tenant_id: TenantId, req: TenantPolicyRequest) -> Result<()> {
        StorageController::tenant_policy(self, tenant_id, req).await
    }

    async fn tenant_shard_split(
        &self,
        tenant_id: TenantId,
        req: TenantShardSplitRequest,
    ) -> Result<TenantShardSplitResponse> {
        StorageController::tenant_shard_split(self, tenant_id, req).await
    }

    async fn tenant_shard_migrate(
        &self,
        tenant_shard_id: TenantShardId,
        req: TenantShardMigrateRequest,
    ) -> Result<TenantShardMigrateResponse> {
        StorageController::tenant_shard_migrate(self, tenant_shard_id, req).await
    }

    async fn tenant_shard_migrate_secondary(
        &self,
        tenant_shard_id: TenantShardId,
        req: TenantShardMigrateRequest,
    ) -> Result<TenantShardMigrateResponse> {
        StorageController::tenant_shard_migrate_secondary(self, tenant_shard_id, req).await
    }

    async fn tenant_shard_cancel_reconcile(&self, tenant_shard_id: TenantShardId) -> Result<()> {
        StorageController::tenant_shard_cancel_reconcile(self, tenant_shard_id).await
    }

    async fn update_preferred_azs(
        &self,
        req: ShardsPreferredAzsRequest,
    ) -> Result<ShardsPreferredAzsResponse> {
        StorageController::update_preferred_azs(self, req).await
    }

    async fn tenant_timeline_create(
        &self,
        tenant_id: TenantId,
        req: TimelineCreateRequest,
    ) -> Result<TimelineCreateResponseStorcon> {
        StorageController::tenant_timeline_create(self, tenant_id, req).await
    }

    async fn tenant_timeline_detail(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> Result<TimelineInfo> {
        StorageController::tenant_timeline_detail(self, tenant_id, timeline_id).await
    }

    async fn set_tenant_config(&self, req: &TenantConfigRequest) -> Result<()> {
        StorageController::set_tenant_config(self, req).await
    }

    async fn node_register(&self, req: NodeRegisterRequest) -> Result<()> {
        StorageController::node_register(self, req).await
    }

    async fn node_configure(&self, req: NodeConfigureRequest) -> Result<()> {
        StorageController::node_configure(self, req).await
    }

    async fn node_list(&self) -> Result<Vec<NodeDescribeResponse>> {
        StorageController::node_list(self).await
    }

    async fn node_drain(&self, node_id: NodeId) -> Result<()> {
        StorageController::node_drain(self, node_id).await
    }

    async fn node_cancel_drain(&self, node_id: NodeId) -> Result<()> {
        StorageController::node_cancel_drain(self, node_id).await
    }

    async fn node_fill(&self, node_id: NodeId) -> Result<()> {
        StorageController::node_fill(self, node_id).await
    }

    async fn node_cancel_fill(&self, node_id: NodeId) -> Result<()> {
        StorageController::node_cancel_fill(self, node_id).await
    }

    async fn node_start_delete(&self, node_id: NodeId, force: bool) -> Result<()> {
        StorageController::node_start_delete(self, node_id, force).await
    }

    async fn safekeeper_list(&self) -> Result<Vec<SafekeeperDescribeResponse>> {
        StorageController::safekeeper_list(self).await
    }

    async fn safekeeper_scheduling(
        &self,
        node_id: NodeId,
        req: SafekeeperSchedulingPolicyRequest,
    ) -> Result<()> {
        StorageController::safekeeper_scheduling_policy(self, node_id, req.scheduling_policy).await
    }

    async fn timeline_safekeeper_migrate(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        req: TimelineSafekeeperMigrateRequest,
    ) -> Result<()> {
        StorageController::timeline_safekeeper_migrate(self, tenant_id, timeline_id, req).await
    }
}

#[derive(Clone)]
pub struct HttpStorageControllerApi {
    base_url: Url,
    client: reqwest::Client,
}

impl HttpStorageControllerApi {
    pub fn new(base_url: Url, client: reqwest::Client) -> Self {
        Self { base_url, client }
    }

    async fn dispatch<RQ, RS>(&self, method: Method, path: String, body: Option<RQ>) -> Result<RS>
    where
        RQ: Serialize + Send + Sync,
        RS: DeserializeOwned + Send,
    {
        let url = self
            .base_url
            .join(path.trim_start_matches('/'))
            .with_context(|| format!("building storage controller URL for {path}"))?;
        let mut request = self.client.request(method, url);
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await?.error_from_body().await?;
        let text = response.text().await?;
        if text.trim().is_empty() {
            serde_json::from_value(serde_json::Value::Null)
                .with_context(|| format!("decoding empty storage controller response for {path}"))
        } else {
            serde_json::from_str(&text)
                .with_context(|| format!("decoding storage controller response for {path}"))
        }
    }
}

#[async_trait]
impl StorageControllerApi for HttpStorageControllerApi {
    async fn tenant_create(&self, req: TenantCreateRequest) -> Result<TenantCreateResponse> {
        self.dispatch(Method::POST, "v1/tenant".to_string(), Some(req))
            .await
    }

    async fn tenant_import(&self, tenant_id: TenantId) -> Result<TenantCreateResponse> {
        self.dispatch::<(), _>(
            Method::POST,
            format!("debug/v1/tenant/{tenant_id}/import"),
            None,
        )
        .await
    }

    async fn tenant_locate(&self, tenant_id: TenantId) -> Result<TenantLocateResponse> {
        self.dispatch::<(), _>(
            Method::GET,
            format!("debug/v1/tenant/{tenant_id}/locate"),
            None,
        )
        .await
    }

    async fn tenant_list(&self, limit: Option<usize>) -> Result<Vec<TenantDescribeResponse>> {
        let path = match limit {
            Some(limit) => format!("control/v1/tenant?limit={limit}"),
            None => "control/v1/tenant".to_string(),
        };
        self.dispatch::<(), _>(Method::GET, path, None).await
    }

    async fn tenant_describe(&self, tenant_id: TenantId) -> Result<TenantDescribeResponse> {
        self.dispatch::<(), _>(Method::GET, format!("control/v1/tenant/{tenant_id}"), None)
            .await
    }

    async fn tenant_delete(&self, tenant_id: TenantId) -> Result<()> {
        self.dispatch::<(), _>(Method::DELETE, format!("v1/tenant/{tenant_id}"), None)
            .await
    }

    async fn tenant_policy(&self, tenant_id: TenantId, req: TenantPolicyRequest) -> Result<()> {
        self.dispatch(
            Method::PUT,
            format!("control/v1/tenant/{tenant_id}/policy"),
            Some(req),
        )
        .await
    }

    async fn tenant_shard_split(
        &self,
        tenant_id: TenantId,
        req: TenantShardSplitRequest,
    ) -> Result<TenantShardSplitResponse> {
        self.dispatch(
            Method::PUT,
            format!("control/v1/tenant/{tenant_id}/shard_split"),
            Some(req),
        )
        .await
    }

    async fn tenant_shard_migrate(
        &self,
        tenant_shard_id: TenantShardId,
        req: TenantShardMigrateRequest,
    ) -> Result<TenantShardMigrateResponse> {
        self.dispatch(
            Method::PUT,
            format!("control/v1/tenant/{tenant_shard_id}/migrate"),
            Some(req),
        )
        .await
    }

    async fn tenant_shard_migrate_secondary(
        &self,
        tenant_shard_id: TenantShardId,
        req: TenantShardMigrateRequest,
    ) -> Result<TenantShardMigrateResponse> {
        self.dispatch(
            Method::PUT,
            format!("control/v1/tenant/{tenant_shard_id}/migrate_secondary"),
            Some(req),
        )
        .await
    }

    async fn tenant_shard_cancel_reconcile(&self, tenant_shard_id: TenantShardId) -> Result<()> {
        self.dispatch::<(), _>(
            Method::PUT,
            format!("control/v1/tenant/{tenant_shard_id}/cancel_reconcile"),
            None,
        )
        .await
    }

    async fn update_preferred_azs(
        &self,
        req: ShardsPreferredAzsRequest,
    ) -> Result<ShardsPreferredAzsResponse> {
        self.dispatch(
            Method::PUT,
            "control/v1/preferred_azs".to_string(),
            Some(req),
        )
        .await
    }

    async fn tenant_timeline_create(
        &self,
        tenant_id: TenantId,
        req: TimelineCreateRequest,
    ) -> Result<TimelineCreateResponseStorcon> {
        self.dispatch(
            Method::POST,
            format!("v1/tenant/{tenant_id}/timeline"),
            Some(req),
        )
        .await
    }

    async fn tenant_timeline_detail(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> Result<TimelineInfo> {
        let response: TenantTimelineDescribeResponse = self
            .dispatch::<(), _>(
                Method::GET,
                format!("control/v1/tenant/{tenant_id}/timeline/{timeline_id}"),
                None,
            )
            .await?;

        response.shards.into_iter().next().with_context(|| {
            format!("timeline {tenant_id}/{timeline_id} describe response has no shards")
        })
    }

    async fn set_tenant_config(&self, req: &TenantConfigRequest) -> Result<()> {
        self.dispatch(Method::PUT, "v1/tenant/config".to_string(), Some(req))
            .await
    }

    async fn node_register(&self, req: NodeRegisterRequest) -> Result<()> {
        self.dispatch(Method::POST, "control/v1/node".to_string(), Some(req))
            .await
    }

    async fn node_configure(&self, req: NodeConfigureRequest) -> Result<()> {
        self.dispatch(
            Method::PUT,
            format!("control/v1/node/{}/config", req.node_id),
            Some(req),
        )
        .await
    }

    async fn node_list(&self) -> Result<Vec<NodeDescribeResponse>> {
        self.dispatch::<(), _>(Method::GET, "control/v1/node".to_string(), None)
            .await
    }

    async fn node_drain(&self, node_id: NodeId) -> Result<()> {
        self.dispatch::<(), _>(
            Method::PUT,
            format!("control/v1/node/{node_id}/drain"),
            None,
        )
        .await
    }

    async fn node_cancel_drain(&self, node_id: NodeId) -> Result<()> {
        self.dispatch::<(), _>(
            Method::DELETE,
            format!("control/v1/node/{node_id}/drain"),
            None,
        )
        .await
    }

    async fn node_fill(&self, node_id: NodeId) -> Result<()> {
        self.dispatch::<(), _>(Method::PUT, format!("control/v1/node/{node_id}/fill"), None)
            .await
    }

    async fn node_cancel_fill(&self, node_id: NodeId) -> Result<()> {
        self.dispatch::<(), _>(
            Method::DELETE,
            format!("control/v1/node/{node_id}/fill"),
            None,
        )
        .await
    }

    async fn node_start_delete(&self, node_id: NodeId, force: bool) -> Result<()> {
        self.dispatch::<(), _>(
            Method::PUT,
            format!("control/v1/node/{node_id}/delete?force={force}"),
            None,
        )
        .await
    }

    async fn safekeeper_list(&self) -> Result<Vec<SafekeeperDescribeResponse>> {
        self.dispatch::<(), _>(Method::GET, "control/v1/safekeeper".to_string(), None)
            .await
    }

    async fn safekeeper_scheduling(
        &self,
        node_id: NodeId,
        req: SafekeeperSchedulingPolicyRequest,
    ) -> Result<()> {
        self.dispatch(
            Method::POST,
            format!("control/v1/safekeeper/{node_id}/scheduling_policy"),
            Some(req),
        )
        .await
    }

    async fn timeline_safekeeper_migrate(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        req: TimelineSafekeeperMigrateRequest,
    ) -> Result<()> {
        self.dispatch(
            Method::POST,
            format!("v1/tenant/{tenant_id}/timeline/{timeline_id}/safekeeper_migrate"),
            Some(req),
        )
        .await
    }
}
