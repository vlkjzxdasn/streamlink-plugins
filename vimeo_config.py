import logging
import re
import time
from urllib.parse import urlparse

from streamlink.plugin import HIGH_PRIORITY, Plugin, pluginargument, pluginmatcher
from streamlink.plugin.api import validate
from streamlink.stream.hls import HLSStream, HLSStreamReader, HLSStreamWorker

log = logging.getLogger(__name__)


def extract_m3u8_from_config(session, config_url):
    """
    Fetches the Vimeo config JSON and extracts the first available HLS URL and expiry.
    Returns a tuple (url, expires).
    """
    log.debug(f"Fetching config from: {config_url}")
    try:
        res = session.http.get(config_url)
        data = session.http.json(res)
    except Exception as e:
        log.error(f"Failed to fetch or parse config: {e}")
        return None, None

    # Schema to extract HLS URLs and expires from the config JSON
    schema_cdns = validate.all(
        {
            "cdns": {
                str: validate.all(
                    {
                        validate.optional("url"): validate.url(),
                        # Fallback to avc_url if url is missing
                        validate.optional("avc_url"): validate.url(),
                    },
                    validate.any(validate.get("url"), validate.get("avc_url")),
                ),
            },
            # Optional: prioritize default_cdn if available
            validate.optional("default_cdn"): str,
        },
    )

    schema = validate.Schema(
        {
            "request": {
                "files": {
                    "hls": schema_cdns
                },
                # Attempt to get expires from request object
                validate.optional("expires"): int,
                validate.optional("timestamp"): int,
            },
            # Fallback attempts for expires if not in request
            validate.optional("expires"): int,
            validate.optional("timestamp"): int,
        },
    )

    try:
        data_parsed = schema.validate(data)
    except Exception as e:
        log.error(f"Failed to validate config schema: {e}")
        return None, None

    if not data_parsed:
        return None, None

    # Extract HLS data
    request_data = data_parsed.get("request", {})
    files_data = request_data.get("files", {})
    hls_data = files_data.get("hls")

    if not hls_data:
        return None, None

    # Extract URL
    url_to_return = None
    cdns = hls_data.get("cdns")
    if cdns:
        # Try to get the default CDN if specified
        default_cdn = hls_data.get("default_cdn")
        if default_cdn and default_cdn in cdns:
            url_to_return = cdns[default_cdn]
        else:
            # Fallback to the first available URL
            for url in cdns.values():
                if url:
                    url_to_return = url
                    break
    
    # Extract expires
    # Priority: request.expires -> root.expires
    expires = request_data.get("expires")
    if expires is None:
        expires = data_parsed.get("expires")

    return url_to_return, expires


class VimeoConfigHLSStreamWorker(HLSStreamWorker):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # config_url is injected into the stream object by the Plugin class
        self.config_url = getattr(self.stream, "config_url", None)
        self.last_refresh = time.time()
        
        # Get refresh interval from arguments
        self.refresh_interval = 30 * 60  # Default 30 minutes
        
        # Try to use initial_expires if available
        initial_expires = getattr(self.stream, "initial_expires", None)
        if initial_expires:
             self.refresh_interval = max(60, int(initial_expires / 2))
             log.info(f"Using initial refresh interval from config (half of expires): {self.refresh_interval}s")

        refresh_arg = self.session.options.get("vimeo-config-refresh-interval")
        if refresh_arg:
            self.refresh_interval = int(refresh_arg)
            log.info(f"Using manual refresh interval: {self.refresh_interval}s")

    def _fetch_playlist(self):
        # Check TTL and refresh if needed
        if self.config_url and (time.time() - self.last_refresh > self.refresh_interval):
            log.info("Refreshing m3u8 URL from config...")
            # Use the session from the worker (self.session)
            new_url, expires = extract_m3u8_from_config(self.session, self.config_url)
            
            # Update refresh interval based on expires if not manually set
            if expires:
                # Use expires - 60s as safety margin, but not less than 60s
                new_interval = max(60, expires - 60)
                
                # Manual refresh-interval overrides everything
                refresh_arg = self.session.options.get("vimeo-config-refresh-interval")
                if refresh_arg:
                    new_interval = int(refresh_arg)
                    log.debug(f"Using manual refresh interval (overriding config expires): {new_interval}s")
                
                if new_interval != self.refresh_interval:
                    self.refresh_interval = new_interval
                    log.info(f"Updated refresh interval to: {self.refresh_interval}s")

            if new_url:
                try:
                    # 1. Get the new CDN Master URL (with new path token) by following redirects
                    # We use stream=True to avoid downloading the body
                    res_cdn = self.session.http.get(new_url, allow_redirects=True, stream=True)
                    new_cdn_url = res_cdn.url
                    res_cdn.close()

                    # 2. Extract the path token (exp=...~hmac=...) from the new CDN URL
                    # The token is a path segment
                    token_match = re.search(r"/(exp=.*?)(?:/|$)", new_cdn_url)
                    if token_match:
                        new_token = token_match.group(1)
                        
                        # 3. Replace the path token in the current media URL
                        current_url = self.stream.url
                        # Regex to find the existing token in the current URL
                        updated_url = re.sub(r"/(exp=.*?)(?:/|$)", f"/{new_token}/", current_url)
                        
                        # Also update query params just in case (e.g. if CDN changed)
                        # But be careful not to overwrite if query params are empty in new URL
                        # Actually, typically the path token is enough for these CDNs.
                        # Let's check if we need to update query params too.
                        # The new CDN URL might have query params.
                        new_cdn_parsed = urlparse(new_cdn_url)
                        updated_url_parsed = urlparse(updated_url)
                        if new_cdn_parsed.query:
                             updated_url = updated_url_parsed._replace(query=new_cdn_parsed.query).geturl()

                        if updated_url != self.stream.url:
                            log.info(f"Updated m3u8 URL token to: {new_token[:20]}...")
                            self.stream.args["url"] = updated_url
                    else:
                        log.warning("Could not find path token in new CDN URL")

                except Exception as e:
                    log.error(f"Failed to resolve CDN URL: {e}")
                
                # Always update last_refresh time if a check was performed, 
                # regardless of whether the URL actually changed.
                self.last_refresh = time.time()
            else:
                log.warning("Failed to refresh m3u8 URL, keeping existing one.")
                # On failure, retry sooner (e.g. 10s)
                self.last_refresh = time.time() - self.refresh_interval + 10

        return super()._fetch_playlist()


class VimeoConfigHLSStreamReader(HLSStreamReader):
    __worker__ = VimeoConfigHLSStreamWorker


class VimeoConfigHLSStream(HLSStream):
    __reader__ = VimeoConfigHLSStreamReader

    def __init__(self, session, url, config_url=None, **kwargs):
        super().__init__(session, url, **kwargs)
        # Store config_url on the stream instance so the worker can access it
        self.config_url = config_url


@pluginmatcher(re.compile(r"https?://player\.vimeo\.com/video/\d+/config\?.*"), priority=HIGH_PRIORITY)
@pluginargument(
    "refresh-interval",
    type=int,
    metavar="SECONDS",
    help="Custom interval in seconds to refresh the Vimeo config and tokens."
)
class VimeoConfig(Plugin):
    def _get_streams(self):
        # 1. Use the shared utility function to get the initial m3u8 URL
        # self.url is the config URL because that's what the plugin matched
        m3u8_url, _ = extract_m3u8_from_config(self.session, self.url)

        if not m3u8_url:
            log.error("No HLS streams found in config")
            return

        log.info(f"Found initial m3u8 URL: {m3u8_url}")

        # 2. Create the custom HLS stream using parse_variant_playlist to support adaptive streams
        # We pass config_url via kwargs so it gets forwarded to the VimeoConfigHLSStream constructor
        yield from VimeoConfigHLSStream.parse_variant_playlist(self.session, m3u8_url, config_url=self.url).items()


__plugin__ = VimeoConfig
