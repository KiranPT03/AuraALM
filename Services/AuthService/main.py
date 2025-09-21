from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from scripts.utils.logger import log
from scripts.config.application import config
from scripts.router.auth_router import auth_router



def subscribe_routes(app):
    app.include_router(auth_router, prefix="/automator/api/v1")


def main():
    # Get FastAPI configuration from config file
    fastapi_config = config.get_fastapi_config()
    
    app = FastAPI(
        title=fastapi_config.get('title', 'Automator API'),
        description=fastapi_config.get('description', 'Automator Services API'),
        version=fastapi_config.get('version', '1.0.0'),
        docs_url=fastapi_config.get('docs_url', '/docs'),
        redoc_url=fastapi_config.get('redoc_url', '/redoc'),
        openapi_url=fastapi_config.get('openapi_url', '/openapi.json')
    )
    
    # Get CORS configuration
    cors_config = fastapi_config.get('cors', {})
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_config.get('allow_origins', ['*']),
        allow_credentials=cors_config.get('allow_credentials', True),
        allow_methods=cors_config.get('allow_methods', ['*']),
        allow_headers=cors_config.get('allow_headers', ['*']),
    )
    
    subscribe_routes(app=app)
    return app


if __name__ == '__main__':
    fastapi_config = config.get_fastapi_config()
    log.info("Starting webhook services")
    uvicorn.run(
        "main:main",
        host=fastapi_config.get('host', '0.0.0.0'),
        port=fastapi_config.get('port', 8000),
        reload=fastapi_config.get('reload', False),
        workers=fastapi_config.get('workers', 1) if not fastapi_config.get('reload', False) else None
    )