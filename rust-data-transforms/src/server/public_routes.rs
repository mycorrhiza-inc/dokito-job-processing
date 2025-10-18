use aide::axum::{
    ApiRouter,
    routing::{get, get_with, post},
};
use mycorrhiza_common::tasks::routing::handle_default_task_route;

use crate::{
    indexes::attachment_url_index::RegenerateUrlAttachIndex,
    server::scraper_check_completed::{get_casedata_differential_with_location, get_raw_caselist},
};
use crate::{indexes::attachment_url_index::handle_attachment_url_lookup, server::s3_routes};

pub fn create_public_router() -> ApiRouter {
    ApiRouter::new()
        .api_route(
            "/debug_case/{state}/{jurisdiction_name}/{docket_govid}",
            get(s3_routes::handle_case_debug_info),
        )
        .api_route(
            "/caselist/{state}/{jurisdiction_name}/casedata_differential/{lookup_stage}",
            post(get_casedata_differential_with_location),
        )
        .api_route(
            "/caselist/{state}/{jurisdiction_name}/all_cases/{lookup_stage}",
            get(get_raw_caselist),
        )
        .api_route(
            "/raw_attachments/{blake2b_hash}/obj",
            get_with(
                s3_routes::handle_attachment_data_from_s3,
                s3_routes::handle_attachment_data_from_s3_docs,
            ),
        )
        .api_route(
            "/raw_attachments/{blake2b_hash}/raw",
            get_with(
                s3_routes::handle_attachment_file_from_s3,
                s3_routes::handle_attachment_file_from_s3_docs,
            ),
        )
        .api_route(
            "/attachment_index/lookup/{url}",
            post(handle_attachment_url_lookup),
        )
        .api_route(
            "/attachment_index/regenerate",
            post(handle_default_task_route::<RegenerateUrlAttachIndex>),
        )
}
