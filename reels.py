import asyncio
import pandas as pd
import json
import re
from datetime import datetime
from playwright.async_api import async_playwright
import time
import os
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials

# Load environment variables
load_dotenv()

class InstagramScraper:    
    def __init__(self):
        # Initialize Google Sheets connection
        self.sheet_id = os.getenv('GOOGLE_SHEET_ID')
        self.sheet_name = os.getenv('GOOGLE_SHEET_NAME', 'Sheet1')
        self.credentials_file = os.getenv('GOOGLE_CREDENTIALS_FILE', 'credentials.json')
        self.sheet_client = None
        self.worksheet = None
        self.setup_google_sheets()

        # Existing initialization
        self.browser = None
        self.context = None
        self.page = None
        self.scraped_data = []
          # Updated selectors for Instagram reels
        self.POST_SELECTORS = [
            'a[href*="/reel/"]',  # Direct reel links
            'div[role="tablist"] a[href*="/reel/"]',  # Reels in tablist
            'div[data-media-type="Reels"] a',  # Reels container
            'div[role="tabpanel"] a[href*="/reel/"]'  # Reels in tab panel
        ]

        # JavaScript to expand truncated content
        self.EXPAND_CONTENT_JS = """
        (async () => {
            // Find and click any "more" buttons in captions
            const moreButtonSelectors = [
                'div._a9zs button',
                'button._aacl._aaco._aacu',
                'button[role="button"]'
            ];

            for (const selector of moreButtonSelectors) {
                const buttons = document.querySelectorAll(selector);
                for (const button of buttons) {
                    const text = button.textContent || '';
                    if (text.includes('more') || text.includes('...')) {
                        console.log('Found more button:', text);
                        button.click();
                        // Wait longer after clicking to ensure expansion completes
                        await new Promise(r => setTimeout(r, 1000));
                    }
                }
            }

            // Wait for possible dynamic content loading
            await new Promise(r => setTimeout(r, 1500));
        })();
        """        
        self.MODAL_SELECTORS = {
            'grid_views': [
                'span[class*="videoViews"]',  # Video views in grid
                'span[class*="view-count"]',  # View count in grid
                'span._ac2a',  # Common view count class
                'span._aacl._aaco',  # Another common view class
                'span:has(svg[aria-label*="view"])',  # View icon with count
                'span:has(svg[aria-label="Play"]) + span'  # Count next to play icon
            ],
            'views': [
                'span[class*="view-count"]',  # Direct view count
                'span:has-text("views")',  # Text containing views
                'span[role="button"]:has-text("views")',  # View count button
                'section span:has-text("views")',  # Views in section
                'div[role="button"] span:has-text("views")'  # Views in button
            ],
            'likes': [
                'section span[role="button"]',  # Primary role-based selector
                'a[role="link"] span[role="button"]',  # Link-based role selector 
                'span[role="button"]',  # Generic role selector
                'div[role="button"] span',  # Nested role selector
                'section div span span:not([role])',  # Generic likes counter
                'a[href*="/liked_by/"] span',  # Liked by link
                'section > div > div > span',  # Covers "Liked by X and others" pattern
                'div[role="presentation"] > div > div > span',  # Presentation role variation
                'article div > span > span',  # Deep nested structure
                'span[aria-label*="like"], span[aria-label*="view"]',  # Aria-labeled engagement
                'div > span > span:not([role])',  # Most generic fallback
                'section div[role="button"]',  # Alternative role structure
                'div[role="button"] div[dir="auto"]',  # Auto-direction text in button
                'section span[aria-label*="like"], section span[aria-label*="view"]',  # Direct access to aria labels
                'article > section span:not([role])'  # Article-specific likes
            ],
            'caption': [
                'h1._aagv span[dir="auto"]',  # Main caption text element
                'h1[dir="auto"]',             # Alternative caption text element
                'div._a9zs span[dir="auto"]',  # Backup caption selector
                'div._a9zs h1',               # Another possible caption container
                'div[role="menuitem"] span',  # Another variation
                'article div._a9zs',          # Container that includes username + caption
                'div.C4VMK > span'           # Legacy selector as fallback
            ],
            'more_button': [
                'div._a9zs button',           # "more" button in caption
                'button._aacl._aaco._aacu',   # Another variation of more button
                'button[role="button"]'       # Generic button fallback
            ],
            'comments': [
                'span._aacl._aaco._aacw._aacz._aada',  # Primary comment count selector
                'section span[aria-label*="comment"]',  # Generic comment count
                'a[href*="/comments/"] span'           # Backup selector for comment counts
            ],
            'date': [
                'time._aaqe[datetime]',
                'time[datetime]'
            ]
        }
        self.user_data_dir = './user_data'  # Add this line
    
    async def setup_browser(self, force_visible=False):
        """Initialize browser with mobile emulation and persistent session
        
        Args:
            force_visible (bool): If True, shows the browser UI. Otherwise runs headless.
        """
        playwright = await async_playwright().start()
        
        # Launch browser with persistent context
        self.context = await playwright.chromium.launch_persistent_context(
            user_data_dir=self.user_data_dir,
            headless=not force_visible,  # Run headless unless force_visible is True
            args=[
                '--no-sandbox',
                '--disable-blink-features=AutomationControlled',
                '--disable-web-security',
                '--disable-features=VizDisplayCompositor'
            ],
            # Mobile device settings
            viewport={'width': 390, 'height': 844},
            user_agent='Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Mobile/15E148 Safari/604.1',
            device_scale_factor=3,
            is_mobile=True,
            has_touch=True
        )
        
        # Create page from persistent context
        self.page = await self.context.new_page()
        
        # Add extra headers
        await self.page.set_extra_http_headers({
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
        print("✅ Browser setup complete")
    
    async def login_instagram(self):
        """Check login status and handle first-time login"""
        try:
            print("🔄 Checking Instagram login status...")
              # Always use visible browser for better compatibility
            await self.setup_browser(force_visible=True)
            await self.page.goto('https://www.instagram.com/', wait_until='networkidle')
            
            # Check if already logged in 
            logged_in = await self.check_login_status()
            
            if logged_in:
                print("✅ Already logged in with saved session!")
                return True
                
            print("📱 Please log in manually...")
              # Give time for the page to fully load
            await asyncio.sleep(2)
            
            # Already in visible mode, so just continue
            
            print("📱 Please log in manually in the browser window...")
            print("⏳ Waiting for login completion (browser will auto-close once logged in)...")
            
            # Wait for successful login
            while not logged_in:
                await asyncio.sleep(2)
                logged_in = await self.check_login_status()
                
            print("✅ Manual login successful!")
            print("💾 Your login session has been saved for future use")
              # Keep browser visible for better compatibility
            return True
                
        except Exception as e:
            print(f"❌ Error during login process: {str(e)}")
            return False
            
    async def check_login_status(self):
        """Check if we're logged into Instagram by looking for multiple indicators"""
        try:
            # Wait briefly for page content
            await asyncio.sleep(2)
            
            # Check for login-required elements
            login_elements = await self.page.query_selector_all('form[action*="login"]')
            if login_elements:
                print("⚠️ Login form detected - not logged in")
                return False
              # Check for home feed indicators
            home_indicators = [
                'a[href*="/p/"]',  # Post links
                'button[aria-label*="Like"]',  # Like buttons
                'svg[aria-label="Home"]',  # Home icon
                'a[href="/"]'  # Home link
            ]
            
            for selector in home_indicators:
                try:
                    element = await self.page.query_selector(selector)
                    if element:
                        return True
                except Exception:
                    continue
            
            # Additional check - look for logged-in user avatar
            avatar = await self.page.query_selector('img[data-testid="user-avatar"]')
            if avatar:
                return True
                
            print("⚠️ No logged-in indicators found")
            return False
            
        except Exception as e:
            print(f"❌ Error checking login status: {str(e)}")
            return False
    async def scrape_profile(self, profile_url):
        """Scrape individual Instagram profile"""
        try:
            print(f"🔄 Scraping: {profile_url}")
            
            # Navigate to profile and wait for load
            await self.page.goto(profile_url, wait_until='networkidle')
            await asyncio.sleep(3)
            
            # Initialize data structure
            profile_data = {
                'username': '',
                'platform': 'Instagram',
                'name': '',
                'phone': '',
                'email': '',
                'description': '',
                'followers': '',
                'avatar': '',
                'totalposts': '',
                'posts': []  # Array to store top 5 posts data
            }
            
            # Extract username from URL
            username_match = re.search(r'instagram\.com/([^/?]+)', profile_url)
            if username_match:
                profile_data['username'] = username_match.group(1)
            
            # Wait for profile elements to load
            try:
                await self.page.wait_for_selector('h2', timeout=10000)
            except:
                print(f"⚠️ Profile elements not loaded for {profile_url}")
            
            # Extract followers count - Fixed regex to capture full number
            try:
                # Get the full page content to search for followers
                page_content = await self.page.content()
                
                # Multiple patterns to match followers count
                followers_patterns = [
                    r'(\d+(?:,\d+)*(?:\.\d+)?[KMB]?)\s+followers',  
                    r'(\d+(?:,\d+)*(?:\.\d+)?[KMB]?)\s*followers',  
                    r'"follower_count":(\d+)',  # JSON format
                ]
                
                for pattern in followers_patterns:
                    followers_match = re.search(pattern, page_content, re.IGNORECASE)
                    if followers_match:
                        followers_count = followers_match.group(1)
                        # Clean up the count
                        if followers_count and not followers_count.lower() in ['followers', 'following']:
                            profile_data['followers'] = followers_count
                            print(f"✅ Found followers: {followers_count}")
                            break
                        
            except Exception as e:
                print(f"⚠️ Could not extract followers: {str(e)}")
            
            # Extract posts count
            try:
                # Look for posts count in the page content
                page_content = await self.page.content()
                posts_patterns = [
                    r'(\d+(?:,\d+)*)\s+posts', 
                    r'(\d+(?:,\d+)*)\s*posts', 
                    r'"media_count":(\d+)', 
                ]
                
                for pattern in posts_patterns:
                    posts_match = re.search(pattern, page_content, re.IGNORECASE)
                    if posts_match:
                        posts_count = posts_match.group(1)
                        # Use parse_count to properly handle numbers with commas
                        profile_data['totalposts'] = self.parse_count(posts_count)
                        print(f"✅ Found total posts: {posts_count}")
                        break
                    
            except Exception as e:
                print(f"⚠️ Could not extract posts count: {str(e)}")
            
            # Extract NAME and DESCRIPTION            try:
                # Look for profile name and bio text
                bio_selectors = [
                    'section header div:last-child div span',
                    'section header div div:last-child span',
                    'div[data-testid="user-bio"]',
                    'section header div:nth-child(2) div:nth-child(3) div span',
                    'section header div:nth-child(2) div:last-child div span',
                    'section div div div:last-child div span:not([aria-label])',
                    'section header > div:nth-child(2) > div:last-child span',
                    'h1[dir="auto"]'
                ]
                
                found_texts = []
                
                for selector in bio_selectors:
                    try:
                        bio_elements = await self.page.query_selector_all(selector)
                        for bio_element in bio_elements:
                            if bio_element:
                                bio_text = await bio_element.text_content()
                                if bio_text and bio_text.strip():
                                    # Skip if it's stats text or username
                                    if (not self.is_stats_text(bio_text) and 
                                        bio_text.strip() != profile_data['username']):
                                        # Add to found texts if it's meaningful
                                        if len(bio_text.strip()) > 3:
                                            found_texts.append(bio_text.strip())
                    except Exception as e:
                        print(f"⚠️ Error extracting text from {selector}: {str(e)}")
                
                # Remove duplicates while preserving order
                unique_texts = []
                for text in found_texts:
                    if text not in unique_texts:
                        unique_texts.append(text)
                        print(f"Found unique text: {text}")
                
                # First text becomes name, second becomes description
                if unique_texts:
                    # Skip texts that are likely navigation or UI elements
                    skip_texts = ['back', 'home', 'posts', 'followers', 'following']
                    
                    # First non-navigation text becomes name
                    for text in unique_texts:
                        if text.lower() not in skip_texts:
                            profile_data['name'] = text
                            print(f"✅ Found name: {text}")
                            break
                            
                    # Next non-navigation, non-name text becomes description
                    if len(unique_texts) > 1:
                        for text in unique_texts[1:]:
                            if (text.lower() not in skip_texts and 
                                text != profile_data['name'] and 
                                not self.is_stats_text(text)):
                                profile_data['description'] = text
                                print(f"✅ Found description: {text}")
                                break
                    
                    # Extract contact info from description first, then name as fallback
                    contact_text = profile_data['description'] if profile_data['description'] else profile_data['name']
                    phone, email = self.extract_contact_info(contact_text)
                    profile_data['phone'] = phone
                    profile_data['email'] = email
                
            except Exception as e:
                print(f"⚠️ Could not extract name/description: {str(e)}")
            
            # Extract avatar/profile picture URL
            try:
                avatar_selectors = [
                    'img[data-testid="user-avatar"]',
                    'img[alt*="profile picture"]',
                    'span img',
                    'header img'
                ]
                
                for selector in avatar_selectors:
                    avatar_element = await self.page.query_selector(selector)
                    if avatar_element:
                        avatar_url = await avatar_element.get_attribute('src')
                        if avatar_url:
                            profile_data['avatar'] = avatar_url
                            break
            except Exception as e:
                print(f"⚠️ Could not extract avatar: {str(e)}")
            
            # Extract top 5 posts using new-tab strategy
            try:
                print("📸 Scraping top 5 posts...")
                print("⏳ Scrolling to load posts...")
                await self.page.mouse.wheel(0, 500)  # scroll distance
                await asyncio.sleep(3)  # wait time after scroll
                  
                # Extract posts using new tab logic
                profile_data = await self.extract_post_data(profile_data)
                
                if not profile_data.get('posts'):
                    print("⚠️ No posts found")
                else:
                    print(f"✅ Successfully scraped {len(profile_data['posts'])} posts")
                    
            except Exception as e:
                print(f"⚠️ Error scraping posts: {str(e)}")
                
            print(f"✅ Successfully scraped: {profile_data['username']}")
            return profile_data
            
        except Exception as e:
            print(f"❌ Error scraping {profile_url}: {str(e)}")
            return None
    
    async def scrape_from_excel(self, excel_file_path):
        """Read Excel file and scrape all profiles"""
        try:
            # Read Excel file
            df = pd.read_excel(excel_file_path)
            print(f"📊 Found {len(df)} profiles to scrape")
            
            url_columns = ['url', 'link', 'profile_url', 'instagram_url']
            url_column = None
            
            for col in url_columns:
                if col in df.columns:
                    url_column = col
                    break
            
            if not url_column:
                print("❌ Could not find URL column in Excel file")
                print(f"Available columns: {list(df.columns)}")
                return
            profile_urls = df[url_column].dropna().tolist()
            
            print(f"🎯 Starting to scrape {len(profile_urls)} profiles...")
            
            for i, url in enumerate(profile_urls, 1):
                print(f"\n[{i}/{len(profile_urls)}] Processing: {url}")
                
                profile_data = await self.scrape_profile(url)
                
                if profile_data:
                    self.scraped_data.append(profile_data)
                
                # Add delay between requests to avoid rate limiting
                if i < len(profile_urls):
                    delay = 5  # 5 seconds delay
                    print(f"⏳ Waiting {delay} seconds before next profile...")
                    await asyncio.sleep(delay)
            
            print(f"\n✅ Scraping complete! Successfully scraped {len(self.scraped_data)} profiles")
            
        except Exception as e:
            print(f"❌ Error reading Excel file: {str(e)}")
      
    def save_results(self):
        """Print scraping summary"""
        try:
            if not self.scraped_data:
                print("❌ No data to save")
                return
            
            # Print summary
            print(f"\n📊 Scraping Summary:")
            print(f"Total profiles scraped: {len(self.scraped_data)}")
            print(f"Profiles with followers data: {sum(1 for item in self.scraped_data if item['followers'])}")
            print(f"Profiles with email: {sum(1 for item in self.scraped_data if item['email'])}")
            print(f"Profiles with phone: {sum(1 for item in self.scraped_data if item['phone'])}")
            
        except Exception as e:
            print(f"❌ Error saving results: {str(e)}")
    
    async def cleanup(self):
        """Close browser but keep session data"""
        if self.context:
            await self.context.close()
        print("🧹 Cleanup complete - Session data preserved")

    async def extract_post_data(self, profile_data):
        """Extract data from top 5 posts of a profile"""
        posts = []
        try:              # Switch to reels tab
            print("🎬 Switching to reels tab...")
            try:
                reels_tab = await self.page.query_selector('a[href*="/reels/"]')
                if reels_tab:
                    await reels_tab.click()
                    await asyncio.sleep(3)  # Wait for tab switch
                    print("✅ Switched to reels tab")
                else:
                    print("⚠️ Could not find reels tab")
                    return profile_data

                # Wait for reels to be visible
                print("🔍 Looking for reels...")
                await self.page.wait_for_selector('a[href*="/reel/"]', timeout=5000)
            except Exception as e:
                print(f"⚠️ Error switching to reels tab: {str(e)}")
            
            # Get page content for debugging
            page_content = await self.page.content()
            print("📄 Page source length:", len(page_content))
            
            for selector in self.POST_SELECTORS:
                try:
                    post_elements = await self.page.query_selector_all(selector)
                    if post_elements and len(post_elements) > 0:
                        print(f"✅ Found {len(post_elements)} post elements using selector: {selector}")
                        # Debug first post element
                        first_post = post_elements[0]
                        href = await first_post.get_attribute('href')
                        print(f"🔗 First post href: {href}")
                        break
                    else:
                        print(f"⚠️ No posts found with selector: {selector}")
                except Exception as e:
                    print(f"⚠️ Error trying selector '{selector}': {str(e)}")
                    continue  # Try next selector
            
            # Check if we found any posts
            if not post_elements or len(post_elements) == 0:
                print("⚠️ No post elements found using any selector")
                return profile_data
                
            # Enhanced grid-based sorting
            print("📊 Analyzing post grid layout...")
            grid_posts = []
            for post_element in post_elements:
                try:
                    box = await post_element.bounding_box()
                    href = await post_element.get_attribute('href')
                    
                    # Only include valid post/reel links
                    if box and href and ('/p/' in href or '/reel/' in href):
                        # Round positions to handle slight misalignments
                        y = round(box['y'] / 10) * 10  # Round to nearest 10 pixels
                        x = round(box['x'] / 10) * 10
                        
                        # Add post type for debugging
                        post_type = 'reel' if '/reel/' in href else 'post'
                        #print(f"Found {post_type} at position x:{x}, y:{y}")
                        
                        grid_posts.append({
                            'y': y,
                            'x': x,
                            'element': post_element,
                            'type': post_type,
                            'href': href
                        })
                except Exception as e:
                    print(f"⚠️ Error processing grid item: {str(e)}")
                    continue
            
            # Sort first by y (row) then x (column)
            sorted_posts = sorted(grid_posts, key=lambda p: (p['y'], p['x']))
            print(f"✅ Grid analysis complete - Found {len(sorted_posts)} items in order")
            
            # Extract just the elements in order
            sorted_elements = [post['element'] for post in sorted_posts]
                
            # Change number of posts to scrape here
            for i, post_element in enumerate(sorted_elements[:3]):                
                post_data = {
                    "type": "reel",
                    "caption": "",
                    "ownerFullName": profile_data.get('name', ''),
                    "ownerUsername": profile_data.get('username', ''),
                    "url": "",
                    "commentsCount": 0,
                    "likesCount": 0,
                    "viewCount": 0,
                    "timestamp": "",
                    "sharesCount": ""
                }
                
                try:
                    # Extract view count from grid first - this is the only place we'll get views
                    print("🔍 Extracting view count from grid...")
                    grid_views = await self.extract_grid_view_count(post_element)
                    if grid_views > 0:
                        post_data['viewCount'] = grid_views
                        print(f"✅ Found grid view count: {grid_views}")
                    else:
                        print("⚠️ No view count found in grid")

                    # Get post URL with safe fallback
                    post_url = await post_element.get_attribute('href')
                    if not post_url:
                        print("⚠️ Could not extract post URL")
                        continue
                        
                    post_data['url'] = f'https://www.instagram.com{post_url}'
                    print(f"🔗 Processing post {i+1}/3: {post_data['url']}")
                    
                    # Open post in new tab - we'll get other data from individual page
                    new_page = await self.context.new_page()
                    await new_page.goto(post_data['url'], wait_until='networkidle')
                    await asyncio.sleep(2)
                    
                    # Expand truncated content
                    print("🔍 Looking for truncated content...")
                    await new_page.evaluate(self.EXPAND_CONTENT_JS)
                    await asyncio.sleep(2)
                    
                    # Extract caption with retries
                    caption_found = False
                    retry_count = 0
                    while not caption_found and retry_count < 3:
                        for selector in self.MODAL_SELECTORS['caption']:
                            try:
                                caption_element = await new_page.query_selector(selector)
                                if caption_element:
                                    caption_text = await caption_element.text_content()
                                    if caption_text:
                                        # Clean up caption
                                        if ':' in caption_text and not caption_text.startswith('http'):
                                            caption_text = ':'.join(caption_text.split(':')[1:]).strip()
                                        caption_text = caption_text.replace('... more', '').strip()
                                        
                                        post_data['caption'] = caption_text
                                        print(f"📝 Found caption: {caption_text[:100]}...")
                                        caption_found = True
                                        break
                            except Exception:
                                continue
                        
                        if not caption_found:
                            retry_count += 1
                            await asyncio.sleep(1)
                    
                    # Extract timestamp
                    for selector in self.MODAL_SELECTORS['date']:
                        try:
                            date_element = await new_page.query_selector(selector)
                            if date_element:
                                timestamp = await date_element.get_attribute('datetime')
                                if timestamp:
                                    post_data['timestamp'] = timestamp
                                    print(f"📅 Found timestamp: {timestamp}")
                                    break
                        except Exception:
                            continue
                    
                    # Extract likes count with retries
                    post_data['likesCount'] = await self.extract_likes_count(new_page)
                    
                    # Extract comments count with retries
                    retry_count = 0
                    while post_data['commentsCount'] == 0 and retry_count < 3:
                        for selector in self.MODAL_SELECTORS['comments']:
                            try:
                                comments_element = await new_page.query_selector(selector)
                                if comments_element:
                                    comments_text = await comments_element.text_content()
                                    if comments_text:
                                        if 'view all' in comments_text.lower():
                                            match = re.search(r'view all (\d+)', comments_text.lower())
                                            if match:
                                                post_data['commentsCount'] = self.parse_count(match.group(1))
                                        else:
                                            numbers = re.findall(r'\d+', comments_text)
                                            if numbers:
                                                post_data['commentsCount'] = self.parse_count(numbers[0])
                                        
                                        if post_data['commentsCount'] > 0:
                                            print(f"💬 Found {post_data['commentsCount']} comments")
                                            break
                            except Exception:
                                continue
                        
                        if post_data['commentsCount'] == 0:
                            retry_count += 1
                            await asyncio.sleep(1)
                    
                    posts.append(post_data)
                    print(f"✅ Successfully extracted post {i+1}/3")
                    
                except Exception as e:
                    print(f"⚠️ Error processing post: {str(e)}")
                    continue
                
                finally:
                    # Always close the new tab
                    if new_page:
                        try:
                            await new_page.close()
                            await asyncio.sleep(1)
                        except Exception as e:
                            print(f"⚠️ Error closing tab: {str(e)}")
            
            # Update profile data
            profile_data['posts'] = posts
            print(f"✅ Successfully extracted {len(posts)} posts")
            
        except Exception as e:
            print(f"❌ Error in post extraction: {str(e)}")
        
        return profile_data    
    async def extract_likes_count(self, new_page):
        """Extract likes count with proper selectors for mobile Instagram"""
        likes_count = 0
        
        try:
            # Wait for the section containing likes to load
            await new_page.wait_for_selector('section', timeout=10000)
            await asyncio.sleep(3)  # Additional wait for dynamic content
            
            # Method 1: Look for visible likes count (like "2,803 likes")
            visible_likes_selectors = [
                'section > div:nth-child(2) > div > div > span',  # Most common location
                'section > div > div > span:has-text("likes")',   # Contains "likes" text
                'section span:has-text("likes")',                # Generic likes text
                'section div > span:has-text("likes")',          # Nested likes text
            ]
            
            for selector in visible_likes_selectors:
                try:
                    likes_element = await new_page.query_selector(selector)
                    if likes_element:
                        likes_text = await likes_element.text_content()
                        if likes_text and 'likes' in likes_text.lower():
                            # Extract number from "2,803 likes" format
                            likes_count = self.parse_count(likes_text)
                            if likes_count > 0:
                                print(f"✅ Found visible likes: {likes_text} = {likes_count}")
                                return likes_count
                except Exception as e:
                    continue
            
            # Method 2: Look for "Liked by X and others" format
            liked_by_selectors = [
                'section span:has-text("Liked by")',
                'section div:has-text("Liked by")',
                'section span:has-text("and others")',
            ]
            
            for selector in liked_by_selectors:
                try:
                    liked_element = await new_page.query_selector(selector)
                    if liked_element:
                        liked_text = await liked_element.text_content()
                        if liked_text and 'liked by' in liked_text.lower():
                            # Extract from "Liked by username and X others"
                            match = re.search(r'and\s+(\d+(?:,\d+)*)\s+others?', liked_text, re.IGNORECASE)
                            if match:
                                others_count = int(match.group(1).replace(',', ''))
                                likes_count = others_count + 1  # +1 for the named user
                                print(f"✅ Found 'liked by' format: {liked_text} = {likes_count}")
                                return likes_count
                            else:
                                # Just "Liked by username and others" without count
                                print(f"⚠️ Hidden likes detected: {liked_text}")
                                return 0  # Hidden likes count
                except Exception as e:
                    continue
            
            # Method 3: Fallback - look for any element with numbers near the heart button
            try:
                # Get all spans in the section
                all_spans = await new_page.query_selector_all('section span')
                for span in all_spans:
                    span_text = await span.text_content()
                    if span_text and re.search(r'\d+.*likes?', span_text, re.IGNORECASE):
                        likes_count = self.parse_count(span_text)
                        if likes_count > 0:
                            print(f"✅ Found likes via fallback: {span_text} = {likes_count}")
                            return likes_count
            except Exception as e:
                print(f"⚠️ Fallback method failed: {str(e)}")
            
            print("⚠️ No likes count found - may be hidden")
            return 0
            
        except Exception as e:
            print(f"❌ Error extracting likes: {str(e)}")
            return 0

    async def extract_views_count(self, new_page):
        """This method is deprecated as we only get views from grid view now"""
        print("⚠️ View count extraction from individual pages is no longer supported")
        return 0

    def parse_count(self, text):
        """Parse number from Instagram text that contains numbers"""
        if not text:
            return 0
            
        text = str(text).strip().lower()
        try:
            # First try to find any numbers in the text
            numbers = re.findall(r'\d+(?:,\d+)*(?:\.\d+)?', text)
            if not numbers:
                return 0
                
            # Take first number found and remove commas
            number_str = numbers[0].replace(',', '')
            
            # Find where the number appears in the text
            number_pos = text.find(number_str)
            if number_pos == -1:  # Shouldn't happen but just in case
                return int(float(number_str))
                
            # Get the text after the number
            text_after_number = text[number_pos + len(number_str):].strip()
            
            base_number = float(number_str)
            
            # Handle K/M/B suffixes
            if text_after_number.startswith('k'):
                return int(base_number * 1000)
            elif text_after_number.startswith('m'):
                return int(base_number * 1000000)
            elif text_after_number.startswith('b'):
                return int(base_number * 1000000000)
            
            # No suffix or suffix is something else (like "likes"), return as is
            return int(base_number)
            
        except Exception as e:
            print(f"⚠️ Error parsing count from '{text}': {str(e)}")
            return 0

    def setup_google_sheets(self):
        """Initialize connection to Google Sheets"""
        try:
            # Set up credentials
            scope = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"
            ]
            
            credentials = Credentials.from_service_account_file(
                self.credentials_file, 
                scopes=scope
            )
            
            # Connect to Google Sheets
            self.sheet_client = gspread.authorize(credentials)
            spreadsheet = self.sheet_client.open_by_key(self.sheet_id)
            self.worksheet = spreadsheet.worksheet(self.sheet_name)
            
            print("✅ Connected to Google Sheet successfully")
            
        except Exception as e:
            print(f"❌ Failed to connect to Google Sheets: {str(e)}")
            raise    
    
    async def scrape_from_sheet(self):
        """Read profile URLs from Google Sheet and scrape them"""
        try:
            # Get all records
            all_data = self.worksheet.get_all_records()
            if not all_data:
                print("❌ No data found in sheet")
                return
                
            # Find the link column
            headers = self.worksheet.row_values(1)
            try:
                link_col_idx = headers.index('link') + 1  # gspread uses 1-based indexing
            except ValueError:
                print("❌ No 'link' column found in sheet")
                return

            # Get all values in link column
            link_col = self.worksheet.col_values(link_col_idx)[1:]  # Skip header
            
            # Create mapping of URLs to row numbers for updating
            row_map = {}  # Maps URLs to row numbers
            profile_urls = []
            
            for row_num, url in enumerate(link_col, start=2):  # Start from row 2 (after header)
                if url and url.strip():
                    profile_urls.append(url.strip())
                    row_map[url.strip()] = row_num
            
            print(f"📊 Found {len(profile_urls)} profiles to scrape")
            
            # Scrape profiles
            for i, url in enumerate(profile_urls, 1):
                print(f"\n[{i}/{len(profile_urls)}] Processing: {url}")
                
                profile_data = await self.scrape_profile(url)
                
                if profile_data:
                    # Update sheet with scraped data
                    await self.update_sheet_row(profile_data, row_map[url])
                    self.scraped_data.append(profile_data)
                
                # Add delay between requests
                if i < len(profile_urls):
                    delay = 5
                    print(f"⏳ Waiting {delay} seconds before next profile...")
                    await asyncio.sleep(delay)
            
            print(f"\n✅ Scraping complete! Successfully scraped {len(self.scraped_data)} profiles")
            
        except Exception as e:
            print(f"❌ Error reading from sheet: {str(e)}")    

    async def update_sheet_row(self, profile_data, row_num):
        """Update a row in the sheet with scraped data"""
        try:
            # Get current headers
            headers = self.worksheet.row_values(1)
            
            # Prepare updates for each column
            updates = []
            
            # Create new headers if they don't exist
            needed_headers = [
                'Username', 'Platform', 'Name', 'Phone', 'Email', 'Description',
                'Followers', 'Avatar URL', 'Total Posts'
            ]
            
            # Create reel-related headers for first 3 reels
            for i in range(1, 4):
                needed_headers.extend([
                    f'Reel {i} URL', f'Reel {i} Caption',
                    f'Reel {i} Likes', f'Reel {i} Comments',
                    f'Reel {i} Views', f'Reel {i} Date'
                ])
            
            # Add any missing headers
            new_headers = False
            for header in needed_headers:
                if header not in headers:
                    headers.append(header)
                    new_headers = True
            
            # Update headers if new ones were added
            if new_headers:
                self.worksheet.update('A1', [headers])
                print("✅ Added new columns to sheet")
            
            # Map profile_data fields to columns
            field_mapping = {
                'username': 'Username',
                'platform': 'Platform',
                'name': 'Name',
                'phone': 'Phone',
                'email': 'Email',
                'description': 'Description',
                'followers': 'Followers',
                'avatar': 'Avatar URL',
                'totalposts': 'Total Posts'
            }
            
            # Update each field if column exists
            for field, column in field_mapping.items():
                try:
                    col_idx = headers.index(column) + 1
                    if field in profile_data:
                        value = profile_data[field]
                        updates.append({
                            'range': f'{gspread.utils.rowcol_to_a1(row_num, col_idx)}',
                            'values': [[value]]
                        })
                except ValueError:
                    # Column doesn't exist - create it
                    headers.append(column)
                    self.worksheet.update('A1', [headers])
                    col_idx = len(headers)
                    value = profile_data.get(field, '')
                    updates.append({
                        'range': f'{gspread.utils.rowcol_to_a1(row_num, col_idx)}',
                        'values': [[value]]
                    })
            
            # Update reels data if any
            if profile_data.get('posts'):
                posts_data = profile_data['posts']
                for i, post in enumerate(posts_data, start=1):
                    # Create/update reel columns
                    post_fields = {
                        f'Reel {i} URL': post.get('url', ''),
                        f'Reel {i} Caption': post.get('caption', ''),
                        f'Reel {i} Likes': post.get('likesCount', ''),
                        f'Reel {i} Comments': post.get('commentsCount', ''),
                        f'Reel {i} Views': post.get('viewCount', ''),
                        f'Reel {i} Date': post.get('timestamp', '')
                    }
                    
                    for field, value in post_fields.items():
                        try:
                            col_idx = headers.index(field) + 1
                        except ValueError:
                            headers.append(field)
                            self.worksheet.update('A1', [headers])
                            col_idx = len(headers)
                        
                        updates.append({
                            'range': f'{gspread.utils.rowcol_to_a1(row_num, col_idx)}',
                            'values': [[value]]
                        })
            
            # Batch update all changes
            if updates:
                self.worksheet.batch_update(updates)
                print(f"✅ Updated row {row_num} in sheet")
                
        except Exception as e:
            print(f"❌ Error updating sheet: {str(e)}")

    async def extract_grid_view_count(self, element):
        """Extract view count from a reel in the grid view"""
        try:
            # First try direct child elements
            for selector in self.MODAL_SELECTORS['grid_views']:
                try:
                    view_element = await element.query_selector(selector)
                    if view_element:
                        view_text = await view_element.text_content()
                        if view_text and ('view' in view_text.lower() or 'k' in view_text.lower() or 'm' in view_text.lower()):
                            # Clean and parse the view count
                            text = view_text.lower().replace('views', '').replace('view', '').strip()
                            if 'k' in text:
                                number = float(text.replace('k', '')) * 1000
                                return int(number)
                            elif 'm' in text:
                                number = float(text.replace('m', '')) * 1000000
                                return int(number)
                            else:
                                count = self.parse_count(text)
                                if count > 0:
                                    return count
                except Exception as e:
                    print(f"⚠️ Grid view selector error: {str(e)}")
                    continue

            # Fallback: Try evaluating JavaScript to find view count
            js_result = await element.evaluate('''
                element => {
                    const findViewCount = () => {
                        const spans = element.querySelectorAll('span');
                        for (const span of spans) {
                            const text = span.textContent;
                            if (text && (text.includes('K') || text.includes('M') || text.includes('view'))) {
                                return text;
                            }
                        }
                        return null;
                    };
                    return findViewCount();
                }
            ''')
            
            if js_result:
                count = self.parse_count(js_result)
                if count > 0:
                    return count

            return 0

        except Exception as e:
            print(f"❌ Error extracting grid view count: {str(e)}")
            return 0

async def main():
    scraper = InstagramScraper()
    
    try:
        print("📊 Connecting to Google Sheets...")
        scraper.setup_google_sheets()  # Setup Google Sheets connection first
        
        print("🌐 Setting up browser...")
        await scraper.setup_browser()
        
        print("🔑 Checking Instagram login...")
        login_success = await scraper.login_instagram()
        
        if not login_success:
            print("❌ Login failed. Exiting...")
            return
        
        # Scrape profiles from Google Sheet
        print("🔄 Starting scraping process...")
        await scraper.scrape_from_sheet()
        
        # Print summary
        scraper.save_results()
        
    except Exception as e:
        print(f"❌ Main execution error: {str(e)}")
    
    finally:
        # Cleanup but preserve session
        await scraper.cleanup()

if __name__ == "__main__":
    print("🚀 Instagram Mobile Scraper Starting...")
    print("=" * 50)
    asyncio.run(main())
