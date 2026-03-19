from pydantic import BaseModel, field_validator


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
        return v
