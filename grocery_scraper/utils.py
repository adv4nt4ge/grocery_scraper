#!/usr/bin/env python3
"""
Utility functions for grocery scraper spiders.
"""

import re
from typing import List


async def init_page_with_blocking(page, request):
    """Initialize page with resource blocking for better performance."""

    async def handle_route(route):
        # Block media and tracking resources
        blocked_patterns = [
            r'\.jpg', r'\.jpeg', r'\.png', r'\.gif', r'\.webp', r'\.svg',
            r'\.mp4', r'\.avi', r'\.mov', r'\.mp3', r'\.wav',
            r'\.woff', r'\.woff2', r'\.ttf', r'\.eot',
            r'google-analytics\.com', r'googletagmanager\.com',
            r'doubleclick\.net', r'facebook\.com'
        ]

        url = route.request.url
        resource_type = route.request.resource_type

        # Block by resource type
        if resource_type in ['image', 'media']:
            await route.abort()
            return

        # Block by URL pattern
        if any(re.search(pattern, url, re.I) for pattern in blocked_patterns):
            await route.abort()
            return

        await route.continue_()

    await page.route('**/*', handle_route)


async def init_vue_page(page, request):
    """Initialize page to wait for Vue.js API responses."""
    # Set up monitoring for the catalog API response
    await page.evaluate('''
        window.vueApiComplete = false;
        window.apiCallCount = 0;
        
        // Monitor network responses using resource event
        const originalFetch = window.fetch;
        window.fetch = function(...args) {
            const response = originalFetch.apply(this, arguments);
            if (args[0] && args[0].includes && args[0].includes('vue_storefront_catalog_2')) {
                window.apiCallCount++;
                response.then(res => {
                    if (res.status === 200) {
                        // Wait a bit for Vue to process the response
                        setTimeout(() => {
                            window.vueApiComplete = true;
                        }, 2000);
                    }
                }).catch(() => {});
            }
            return response;
        };
        
        // Fallback: Mark as complete after 15 seconds regardless
        setTimeout(() => {
            window.vueApiComplete = true;
        }, 15000);
    ''')


async def init_spa_page(page, request):
    """Initialize page for Single Page Applications with resource blocking."""
    # First apply resource blocking
    await init_page_with_blocking(page, request)
    
    # Then wait for SPA to load
    await page.wait_for_load_state('networkidle')


def get_blocked_resource_patterns() -> List[str]:
    """Get list of resource patterns to block for better performance."""
    return [
        r'\.jpg', r'\.jpeg', r'\.png', r'\.gif', r'\.webp', r'\.svg',
        r'\.mp4', r'\.avi', r'\.mov', r'\.mp3', r'\.wav',
        r'\.woff', r'\.woff2', r'\.ttf', r'\.eot',
        r'google-analytics\.com', r'googletagmanager\.com',
        r'doubleclick\.net', r'facebook\.com', r'googlesyndication\.com',
        r'facebook\.net', r'twitter\.com', r'linkedin\.com'
    ]