"""Tests for constants."""

from custom_components.jebao_aqua.const import (
    DOMAIN,
    GIZWITS_APP_ID,
    GIZWITS_API_URLS,
    DEFAULT_REGION,
    TOKEN_REFRESH_MARGIN,
    MAX_LAN_FAILURES,
    SERVICE_MAP,
)


class TestConstants:
    def test_domain(self):
        assert DOMAIN == "jebao_aqua"

    def test_app_id(self):
        assert len(GIZWITS_APP_ID) == 32

    def test_default_region(self):
        assert DEFAULT_REGION == "eu"

    def test_all_regions_have_required_urls(self):
        required_keys = {"LOGIN_URL", "DEVICES_URL", "DEVICE_DATA_URL", "CONTROL_URL", "REFRESH_TOKEN_URL"}
        for region, urls in GIZWITS_API_URLS.items():
            for key in required_keys:
                assert key in urls, f"Missing {key} in region {region}"
                assert urls[key].startswith("https://"), f"{key} in {region} should use HTTPS"

    def test_token_refresh_margin(self):
        assert TOKEN_REFRESH_MARGIN == 7 * 24 * 3600

    def test_max_lan_failures(self):
        assert MAX_LAN_FAILURES == 3

    def test_service_map_has_major_countries(self):
        assert "US" in SERVICE_MAP
        assert "CN" in SERVICE_MAP
        assert "GB" in SERVICE_MAP
        assert "DE" in SERVICE_MAP

    def test_service_map_values_are_valid_regions(self):
        valid_regions = set(GIZWITS_API_URLS.keys())
        for country, region in SERVICE_MAP.items():
            assert region in valid_regions, f"Country {country} maps to invalid region {region}"

    def test_refresh_token_urls_match_region_patterns(self):
        assert "euapi.gizwits.com" in GIZWITS_API_URLS["eu"]["REFRESH_TOKEN_URL"]
        assert "usapi.gizwits.com" in GIZWITS_API_URLS["us"]["REFRESH_TOKEN_URL"]
        assert "api.gizwits.com" in GIZWITS_API_URLS["cn"]["REFRESH_TOKEN_URL"]
