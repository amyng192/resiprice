import ipaddress
import socket
from urllib.parse import urlparse

from pydantic import BaseModel, field_validator

# Private/reserved networks that should never be scraped
_BLOCKED_NETWORKS = [
    ipaddress.ip_network("0.0.0.0/8"),
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("100.64.0.0/10"),
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.0.0.0/24"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("198.18.0.0/15"),
    ipaddress.ip_network("::1/128"),
    ipaddress.ip_network("fc00::/7"),
    ipaddress.ip_network("fe80::/10"),
]


def _is_private_host(hostname: str) -> bool:
    """Check if a hostname resolves to a private/reserved IP."""
    try:
        addr_info = socket.getaddrinfo(hostname, None)
        for _, _, _, _, sockaddr in addr_info:
            ip = ipaddress.ip_address(sockaddr[0])
            if any(ip in net for net in _BLOCKED_NETWORKS):
                return True
    except (socket.gaierror, ValueError):
        # If we can't resolve, block it to be safe
        return True
    return False


class ScrapeRequest(BaseModel):
    urls: list[str]

    @field_validator("urls")
    @classmethod
    def validate_urls(cls, v):
        if len(v) < 1:
            raise ValueError("At least 1 URL is required")
        if len(v) > 4:
            raise ValueError("Maximum 4 URLs allowed")
        for url in v:
            if not url.startswith(("http://", "https://")):
                raise ValueError(f"Invalid URL: {url}")
            parsed = urlparse(url)
            hostname = parsed.hostname or ""
            if not hostname:
                raise ValueError(f"Invalid URL (no host): {url}")
            if _is_private_host(hostname):
                raise ValueError(f"URLs targeting internal networks are not allowed")
        return v
