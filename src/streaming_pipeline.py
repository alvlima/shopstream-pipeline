import json
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
from collections import defaultdict, deque
import threading
import queue
import redis
from kafka import KafkaConsumer, KafkaProducer
import pandas as pd
from prometheus_client import Counter, Histogram, Gauge

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Prometheus metrics
events_processed = Counter('events_processed_total', 'Total events processed')
events_per_minute = Gauge('events_per_minute', 'Events processed per minute')
unique_users_per_minute = Gauge('unique_users_per_minute', 'Unique users per minute')
processing_latency = Histogram('processing_latency_seconds', 'Event processing latency')
anomalies_detected = Counter('anomalies_detected_total', 'Total anomalies detected', ['type'])
active_sessions = Gauge('active_sessions', 'Number of active user sessions')

@dataclass
class ClickstreamEvent:
    """Clickstream event data structure - Updated for real ShopStream data"""
    event_id: str
    user_id: Optional[str]
    session_id: str
    timestamp: str  # ISO format timestamp
    event_type: str
    page_url: str
    user_agent: str
    ip_address: str
    device_type: str
    country: str
    referrer: Optional[str] = None
    properties: Optional[Dict] = None
    
    @classmethod
    def from_json(cls, json_data: Dict) -> 'ClickstreamEvent':
        """Create ClickstreamEvent from JSON data"""
        # Handle the real data format
        return cls(
            event_id=json_data.get('event_id'),
            user_id=json_data.get('user_id'),
            session_id=json_data.get('session_id'),
            timestamp=json_data.get('timestamp'),
            event_type=json_data.get('event_type'),
            page_url=json_data.get('page_url'),
            user_agent=json_data.get('user_agent'),
            ip_address=json_data.get('ip_address'),
            device_type=json_data.get('device_type'),
            country=json_data.get('country'),
            referrer=json_data.get('referrer'),
            properties=json_data.get('properties', {})
        )
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return asdict(self)
    
    def get_timestamp_unix(self) -> int:
        """Convert ISO timestamp to Unix timestamp"""
        from datetime import datetime
        dt = datetime.fromisoformat(self.timestamp.replace('Z', '+00:00'))
        return int(dt.timestamp())
    
    def is_bot_traffic(self) -> bool:
        """Detect bot traffic based on user agent"""
        bot_indicators = ['bot', 'crawler', 'spider', 'scraper']
        return any(indicator in self.user_agent.lower() for indicator in bot_indicators)
    
    def is_suspicious_behavior(self) -> bool:
        """Detect suspicious behavior patterns"""
        if self.is_bot_traffic():
            return True
        
        # Check for abnormally high quantities
        if self.properties and self.properties.get('quantity', 0) > 50:
            return True
        
        # Check for abnormally high cart values
        if self.properties and self.properties.get('cart_value', 0) > 50000:
            return True
        
        # Check for suspicious user IDs
        if self.user_id and 'suspicious' in self.user_id.lower():
            return True
        
        return False

@dataclass
class UserSession:
    """User session data structure"""
    session_id: str
    user_id: str
    start_time: int
    last_activity: int
    event_count: int = 0
    page_views: int = 0
    unique_pages: set = None
    device_type: str = ""
    ip_address: str = ""
    is_suspicious: bool = False
    
    def __post_init__(self):
        if self.unique_pages is None:
            self.unique_pages = set()
    
    def update_activity(self, event: ClickstreamEvent):
        """Update session with new event"""
        self.last_activity = event.get_timestamp_unix()
        self.event_count += 1
        
        if event.event_type == 'page_view':
            self.page_views += 1
            self.unique_pages.add(event.page_url)
        
        if not self.device_type:
            self.device_type = event.device_type
        if not self.ip_address:
            self.ip_address = event.ip_address
        
        # Check for suspicious activity
        if event.is_suspicious_behavior():
            self.is_suspicious = True
    
    def is_expired(self, current_time: int, timeout_minutes: int = 30) -> bool:
        """Check if session has expired"""
        timeout_seconds = timeout_minutes * 60
        return (current_time - self.last_activity) > timeout_seconds
    
    def duration_minutes(self) -> float:
        """Get session duration in minutes"""
        return (self.last_activity - self.start_time) / 60

class AnomalyDetector:
    """Detect anomalous user behavior"""
    
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        self.thresholds = {
            'events_per_minute': 100,
            'unique_ips_per_user': 5,
            'sessions_per_minute': 10,
            'page_views_per_minute': 200
        }
    
    def detect_anomalies(self, event: ClickstreamEvent) -> List[str]:
        """Detect anomalies for given event - Updated for real data"""
        anomalies = []
        
        # Skip processing for events without user_id (anonymous users)
        if not event.user_id:
            return anomalies
        
        current_minute = event.get_timestamp_unix() // 60
        
        # Check for bot traffic
        if event.is_bot_traffic():
            anomalies.append('bot_traffic')
            anomalies_detected.labels(type='bot_traffic').inc()
        
        # Check for suspicious behavior patterns
        if event.is_suspicious_behavior():
            anomalies.append('suspicious_behavior')
            anomalies_detected.labels(type='suspicious_behavior').inc()
        
        # Check events per minute per user
        user_events_key = f"user_events:{event.user_id}:{current_minute}"
        event_count = self.redis.incr(user_events_key)
        self.redis.expire(user_events_key, 120)  # Expire after 2 minutes
        
        if event_count > self.thresholds['events_per_minute']:
            anomalies.append('high_event_frequency')
            anomalies_detected.labels(type='high_event_frequency').inc()
        
        # Check unique IPs per user
        user_ips_key = f"user_ips:{event.user_id}:{current_minute}"
        self.redis.sadd(user_ips_key, event.ip_address)
        ip_count = self.redis.scard(user_ips_key)
        self.redis.expire(user_ips_key, 120)
        
        if ip_count > self.thresholds['unique_ips_per_user']:
            anomalies.append('multiple_ips')
            anomalies_detected.labels(type='multiple_ips').inc()
        
        # Check for high-value cart additions
        if event.event_type == 'add_to_cart' and event.properties:
            cart_value = event.properties.get('cart_value', 0)
            quantity = event.properties.get('quantity', 0)
            
            if cart_value > 10000:  # High value threshold
                anomalies.append('high_value_transaction')
                anomalies_detected.labels(type='high_value_transaction').inc()
            
            if quantity > 20:  # High quantity threshold
                anomalies.append('bulk_purchase')
                anomalies_detected.labels(type='bulk_purchase').inc()
        
        return anomalies

class SessionManager:
    """Manage user sessions with expiration"""
    
    def __init__(self, redis_client: redis.Redis, session_timeout: int = 30):
        self.redis = redis_client
        self.session_timeout = session_timeout  # minutes
        self.sessions: Dict[str, UserSession] = {}
        self.session_lock = threading.Lock()
    
    def get_or_create_session(self, event: ClickstreamEvent) -> UserSession:
        """Get existing session or create new one - Updated for real data"""
        # Skip session management for events without user_id
        if not event.user_id:
            # Create temporary session for anonymous users
            return UserSession(
                session_id=event.session_id,
                user_id="anonymous",
                start_time=event.get_timestamp_unix(),
                last_activity=event.get_timestamp_unix(),
                device_type=event.device_type,
                ip_address=event.ip_address
            )
        
        with self.session_lock:
            session_key = f"{event.user_id}:{event.session_id}"
            
            if session_key in self.sessions:
                session = self.sessions[session_key]
                if not session.is_expired(event.get_timestamp_unix(), self.session_timeout):
                    return session
                else:
                    # Session expired, create new one
                    self._finalize_session(session)
                    del self.sessions[session_key]
            
            # Create new session
            session = UserSession(
                session_id=event.session_id,
                user_id=event.user_id,
                start_time=event.get_timestamp_unix(),
                last_activity=event.get_timestamp_unix(),
                device_type=event.device_type,
                ip_address=event.ip_address
            )
            
            self.sessions[session_key] = session
            return session
    
    def _finalize_session(self, session: UserSession):
        """Finalize and store completed session"""
        session_data = {
            'session_id': session.session_id,
            'user_id': session.user_id,
            'start_time': session.start_time,
            'end_time': session.last_activity,
            'duration_minutes': session.duration_minutes(),
            'event_count': session.event_count,
            'page_views': session.page_views,
            'unique_pages_count': len(session.unique_pages),
            'device_type': session.device_type,
            'ip_address': session.ip_address,
            'is_suspicious': session.is_suspicious
        }
        
        # Store in Redis for real-time access
        session_key = f"completed_session:{session.session_id}"
        self.redis.setex(session_key, 3600, json.dumps(session_data))  # 1 hour TTL
        
        logger.info(f"Session finalized: {session.session_id}, "
                   f"duration: {session.duration_minutes():.1f}min, "
                   f"events: {session.event_count}")
    
    def cleanup_expired_sessions(self):
        """Clean up expired sessions"""
        current_time = int(time.time())
        expired_sessions = []
        
        with self.session_lock:
            for session_key, session in self.sessions.items():
                if session.is_expired(current_time, self.session_timeout):
                    expired_sessions.append((session_key, session))
            
            for session_key, session in expired_sessions:
                self._finalize_session(session)
                del self.sessions[session_key]
        
        if expired_sessions:
            logger.info(f"Cleaned up {len(expired_sessions)} expired sessions")

class MetricsCalculator:
    """Calculate real-time metrics"""
    
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        self.event_buffer = deque(maxlen=1000)  # Keep last 1000 events
        self.unique_users_buffer = set()
        self.last_minute_reset = 0
    
    def update_metrics(self, event: ClickstreamEvent):
        """Update real-time metrics - Updated for real data"""
        current_minute = event.get_timestamp_unix() // 60
        
        # Reset minute-based metrics
        if current_minute != self.last_minute_reset:
            self._reset_minute_metrics()
            self.last_minute_reset = current_minute
        
        # Add event to buffer
        self.event_buffer.append(event)
        
        # Only count users with valid user_id
        if event.user_id:
            self.unique_users_buffer.add(event.user_id)
        
        # Update Prometheus metrics
        events_processed.inc()
        
        # Calculate events per minute (from buffer)
        current_time = event.get_timestamp_unix()
        minute_ago = current_time - 60
        recent_events = [e for e in self.event_buffer if e.get_timestamp_unix() >= minute_ago]
        events_per_minute.set(len(recent_events))
        
        # Update unique users per minute
        unique_users_per_minute.set(len(self.unique_users_buffer))
        
        # Store metrics in Redis with additional breakdown
        metrics_key = f"metrics:{current_minute}"
        event_type_counts = {}
        for e in recent_events:
            event_type_counts[e.event_type] = event_type_counts.get(e.event_type, 0) + 1
        
        self.redis.hset(metrics_key, mapping={
            'events_count': len(recent_events),
            'unique_users': len(self.unique_users_buffer),
            'page_views': event_type_counts.get('page_view', 0),
            'add_to_cart': event_type_counts.get('add_to_cart', 0),
            'searches': event_type_counts.get('search', 0),
            'timestamp': current_time
        })
        self.redis.expire(metrics_key, 3600)  # 1 hour TTL
    
    def _reset_minute_metrics(self):
        """Reset minute-based metrics"""
        self.unique_users_buffer.clear()

class StreamingPipeline:
    """Main streaming pipeline orchestrator"""
    
    def __init__(self, config: Dict):
        self.config = config
        
        # Initialize Redis
        self.redis = redis.Redis(
            host=config.get('redis_host', 'localhost'),
            port=config.get('redis_port', 6379),
            db=config.get('redis_db', 0),
            decode_responses=True
        )
        
        # Initialize components
        self.session_manager = SessionManager(self.redis)
        self.anomaly_detector = AnomalyDetector(self.redis)
        self.metrics_calculator = MetricsCalculator(self.redis)
        
        # Initialize Kafka
        self.consumer = KafkaConsumer(
            config.get('input_topic', 'clickstream-events'),
            bootstrap_servers=config.get('kafka_servers', ['localhost:9092']),
            auto_offset_reset='latest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id=config.get('consumer_group', 'streaming-pipeline')
        )
        
        self.producer = KafkaProducer(
            bootstrap_servers=config.get('kafka_servers', ['localhost:9092']),
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Processing stats
        self.processing_stats = {
            'events_processed': 0,
            'anomalies_detected': 0,
            'sessions_created': 0,
            'start_time': time.time()
        }
        
        # Start cleanup thread
        self.cleanup_thread = threading.Thread(target=self._cleanup_worker, daemon=True)
        self.cleanup_thread.start()
        
        logger.info("Streaming pipeline initialized")
    
    def process_event(self, event_data: Dict) -> bool:
        """Process single clickstream event"""
        try:
            start_time = time.time()
            
            # Parse event
            event = ClickstreamEvent.from_json(event_data)
            
            # Update metrics
            self.metrics_calculator.update_metrics(event)
            
            # Get or create session
            session = self.session_manager.get_or_create_session(event)
            session.update_activity(event)
            
            # Detect anomalies
            anomalies = self.anomaly_detector.detect_anomalies(event)
            if anomalies:
                session.is_suspicious = True
                self.processing_stats['anomalies_detected'] += 1
                
                # Publish anomaly alert
                alert_data = {
                    'user_id': event.user_id,
                    'session_id': event.session_id,
                    'timestamp': event.timestamp,
                    'anomalies': anomalies,
                    'event_data': event.to_dict()
                }
                
                self.producer.send('fraud-alerts', value=alert_data)
                logger.warning(f"Anomaly detected for user {event.user_id}: {anomalies}")
            
            # Update processing stats
            self.processing_stats['events_processed'] += 1
            
            # Record processing latency
            processing_time = time.time() - start_time
            processing_latency.observe(processing_time)
            
            # Update active sessions gauge
            active_sessions.set(len(self.session_manager.sessions))
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing event: {e}", exc_info=True)
            return False
    
    def run(self):
        """Main processing loop"""
        logger.info("Starting streaming pipeline...")
        
        try:
            for message in self.consumer:
                self.process_event(message.value)
                
                # Log stats periodically
                if self.processing_stats['events_processed'] % 1000 == 0:
                    self._log_stats()
                    
        except KeyboardInterrupt:
            logger.info("Shutting down pipeline...")
        except Exception as e:
            logger.error(f"Pipeline error: {e}", exc_info=True)
        finally:
            self.consumer.close()
            self.producer.close()
    
    def _cleanup_worker(self):
        """Background worker for cleanup tasks"""
        while True:
            try:
                # Clean up expired sessions every 5 minutes
                self.session_manager.cleanup_expired_sessions()
                time.sleep(300)  # 5 minutes
            except Exception as e:
                logger.error(f"Cleanup worker error: {e}", exc_info=True)
                time.sleep(60)  # Wait 1 minute before retry
    
    def _log_stats(self):
        """Log processing statistics"""
        runtime = time.time() - self.processing_stats['start_time']
        events_per_sec = self.processing_stats['events_processed'] / runtime
        
        logger.info(
            f"Pipeline stats - Events: {self.processing_stats['events_processed']}, "
            f"Anomalies: {self.processing_stats['anomalies_detected']}, "
            f"Rate: {events_per_sec:.1f} events/sec, "
            f"Active sessions: {len(self.session_manager.sessions)}"
        )

class DataGenerator:
    """Generate sample clickstream data for testing"""
    
    def __init__(self, kafka_producer: KafkaProducer):
        self.producer = kafka_producer
        self.user_ids = [f"usr_{i:04d}" for i in range(1, 101)]  # 100 users
        self.pages = [
            '/home', '/products', '/cart', '/checkout', '/profile',
            '/category/electronics', '/category/clothing', '/search',
            '/product/123', '/product/456', '/about', '/contact'
        ]
        self.device_types = ['desktop', 'mobile', 'tablet']
        self.event_types = ['page_view', 'click', 'scroll', 'add_to_cart', 'purchase']
        self.countries = ['US', 'CA', 'UK', 'DE', 'FR']
    
    def generate_event(self, user_id: str = None, anomalous: bool = False) -> Dict:
        """Generate a single clickstream event"""
        import random
        
        if not user_id:
            user_id = random.choice(self.user_ids)
        
        timestamp = datetime.utcnow().isoformat() + 'Z'
        session_id = f"ses_{user_id}_{int(time.time()) // 1800}"  # 30-min sessions
        
        event = {
            'event_id': f"evt_{int(time.time() * 1000000)}_{random.randint(1000, 9999)}",
            'user_id': user_id,
            'session_id': session_id,
            'timestamp': timestamp,
            'event_type': random.choice(self.event_types),
            'page_url': random.choice(self.pages),
            'user_agent': 'Mozilla/5.0 (compatible; ShopStream/1.0)',
            'ip_address': f"192.168.{random.randint(1,255)}.{random.randint(1,255)}",
            'device_type': random.choice(self.device_types),
            'country': random.choice(self.countries),
            'referrer': random.choice([None, 'google.com', 'facebook.com']),
            'properties': {
                'campaign': random.choice(['summer_sale', 'black_friday', None]),
                'ab_test': random.choice(['A', 'B', None])
            }
        }
        
        # Generate anomalous behavior
        if anomalous:
            # Rapid clicks from same user
            event['user_id'] = 'usr_suspicious_001'
            event['user_agent'] = 'Bot/1.0'
            event['ip_address'] = f"10.0.{random.randint(1,10)}.{random.randint(1,255)}"
            if event['event_type'] == 'add_to_cart':
                event['properties'].update({
                    'quantity': random.randint(100, 1000),
                    'cart_value': random.randint(50000, 100000)
                })
        
        return event
    
    def generate_load(self, events_per_second: int = 100, duration_seconds: int = 60):
        """Generate sustained load for testing"""
        import random
        
        logger.info(f"Generating {events_per_second} events/sec for {duration_seconds} seconds")
        
        start_time = time.time()
        event_count = 0
        
        while time.time() - start_time < duration_seconds:
            batch_start = time.time()
            
            # Generate events for this second
            for _ in range(events_per_second):
                # 5% chance of anomalous event
                anomalous = random.random() < 0.05
                event = self.generate_event(anomalous=anomalous)
                
                self.producer.send('clickstream-events', value=event)
                event_count += 1
            
            # Wait for next second
            elapsed = time.time() - batch_start
            if elapsed < 1.0:
                time.sleep(1.0 - elapsed)
            
            if event_count % 1000 == 0:
                logger.info(f"Generated {event_count} events")
        
        logger.info(f"Load generation complete: {event_count} events")

def create_config() -> Dict:
    """Create pipeline configuration"""
    return {
        'kafka_servers': ['localhost:9092'],
        'input_topic': 'clickstream-events',
        'consumer_group': 'streaming-pipeline',
        'redis_host': 'localhost',
        'redis_port': 6379,
        'redis_db': 0,
        'session_timeout_minutes': 30,
        'metrics_port': 8000
    }

def load_sample_data() -> List[Dict]:
    """Load real sample clickstream data from JSON file"""
    sample_events = [
        {
            "event_id": "evt_test_001",
            "user_id": "usr_abc123",
            "session_id": "ses_xyz789",
            "timestamp": "2024-01-15T10:23:45.123Z",
            "event_type": "page_view",
            "page_url": "/products/laptop-123",
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "ip_address": "192.168.1.100",
            "referrer": "google.com",
            "device_type": "desktop",
            "country": "US",
            "properties": {
                "product_id": "prod_laptop_123",
                "category": "electronics",
                "price": 999.99
            }
        },
        {
            "event_id": "evt_test_002",
            "user_id": "usr_abc123",
            "session_id": "ses_xyz789",
            "timestamp": "2024-01-15T10:24:12.456Z",
            "event_type": "add_to_cart",
            "page_url": "/products/laptop-123",
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "ip_address": "192.168.1.100",
            "device_type": "desktop",
            "country": "US",
            "properties": {
                "product_id": "prod_laptop_123",
                "quantity": 1,
                "cart_value": 999.99
            }
        },
        {
            "event_id": "evt_test_003",
            "user_id": "usr_suspicious",
            "session_id": "ses_ddd444",
            "timestamp": "2024-01-15T11:10:00.000Z",
            "event_type": "add_to_cart",
            "page_url": "/products/gpu-999",
            "user_agent": "Bot/1.0",
            "ip_address": "1.2.3.4",
            "device_type": "desktop",
            "country": "XX",
            "properties": {
                "product_id": "prod_gpu_999",
                "quantity": 100,
                "cart_value": 79999.00
            }
        }
    ]
    return sample_events

def load_real_data_from_files() -> List[Dict]:
    """Load real data from provided JSON files"""
    try:
        with open('data/sample/clickstream_events.json', 'r') as f:
            events = json.load(f)
        return events
    except FileNotFoundError:
        logger.warning("Real data files not found, using sample data")
        return load_sample_data()

def test_pipeline(config: Dict):
    """Test pipeline functionality"""
    logger.info("Running pipeline tests...")
    
    # Initialize components
    redis_client = redis.Redis(decode_responses=True)
    session_manager = SessionManager(redis_client)
    anomaly_detector = AnomalyDetector(redis_client)
    
    # Test data
    sample_events = load_sample_data()
    
    # Test session management
    for event_data in sample_events:
        event = ClickstreamEvent.from_json(event_data)
        session = session_manager.get_or_create_session(event)
        session.update_activity(event)
        
        # Test anomaly detection
        anomalies = anomaly_detector.detect_anomalies(event)
        
        logger.info(f"Processed event: {event.event_type}, "
                   f"Session: {session.session_id}, "
                   f"Anomalies: {anomalies}")
    
    # Test session cleanup
    session_manager.cleanup_expired_sessions()
    
    logger.info("Pipeline tests completed successfully")