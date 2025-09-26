import uuid

from scripts.utils.logger import log
from scripts.utils.rest_errors import RestErrors
from scripts.databases.mongodb.client import MongoClient
from scripts.services.jwt import JWTService
from scripts.utils.commons import Commons
from scripts.models.user import User
from scripts.models.response import ErrorDetail
from scripts.models.request import UserProfile
from scripts.models.organization import Organization


class UserService:
    def __init__(self, config):
        self.config = config
        mongo_config = self.config.get_mongodb_config()
        self.mongo_client = MongoClient(mongo_config)
        self.jwt_service = JWTService(config)

    def _load_organization_data(self, org_id: str):
        """
        Load organization data from database and cache it.
        This is called once during initialization to avoid repeated database calls.
        
        Args:
            org_id: The organization ID to fetch data for
        """
        try:
            org_data = self.mongo_client.find_one(
                "organizations",
                {"org_id": org_id}
            )
            
            if org_data:
                # Convert to Organization model for validation and structure
                del org_data['_id']
                self.org_data = Organization(**org_data)
                log.info(f"Organization data loaded successfully for org_id: {org_id}")
            else:
                log.warning(f"Organization not found for org_id: {org_id}")
                self.org_data = None
                
        except Exception as e:
            log.error(f"Failed to load organization data for org_id {org_id}: {str(e)}")
            self.org_data = None

    def get_users(self, logged_user: UserProfile, limit: int = 100, skip: int = 0):
        """
        Retrieve all users with comprehensive validation, pagination, and error handling.
        
        Args:
            current_user (UserProfile): The authenticated user making the request
            limit (int): Maximum number of users to return (default: 100, max: 1000)
            skip (int): Number of users to skip for pagination (default: 0)
            
        Returns:
            dict: Response with success/error status and list of user data
        """
        try:
            # Input validation
            org_id = logged_user.org_id
            self._load_organization_data(org_id)
            
            if not self.org_data or self.org_data.status != 'active':
                log.warning(f"Get users failed: Invalid or inactive organization {org_id}")
                error_detail = ErrorDetail(
                    code="INVALID_ORGANIZATION",
                    message="Invalid or inactive organization",
                    field="org_id"
                )
                return RestErrors.bad_request_400(
                    message="Invalid or inactive organization",
                    data={"org_id": org_id},
                    errors=[error_detail]
                )
            
            # Validate pagination parameters
            if limit < 1 or limit > 1000:
                error_detail = ErrorDetail(
                    code="INVALID_LIMIT",
                    message="Limit must be between 1 and 1000",
                    field="limit"
                )
                return RestErrors.bad_request_400(
                    message="Invalid limit parameter",
                    data=None,
                    errors=[error_detail]
                )
            
            if skip < 0:
                error_detail = ErrorDetail(
                    code="INVALID_SKIP",
                    message="Skip must be 0 or greater",
                    field="skip"
                )
                return RestErrors.bad_request_400(
                    message="Invalid skip parameter",
                    data=None,
                    errors=[error_detail]
                )
            
            log.info(f"Retrieving users for organization: {org_id}, limit: {limit}, skip: {skip}")
            
            # Query users from database with organization filter
            try:
                # Build query filter for organization
                # TODO : In case of multi tenancy application
                # query_filter = {"organization.org_id": org_id}

                query_filter = {}
                
                # Get total count for pagination metadata
                total_count = self.mongo_client.count_documents("users", query_filter)
                
                # Query users with pagination
                users_data = self.mongo_client.find_many(
                    "users", 
                    query_filter, 
                    limit=limit, 
                    skip=skip,
                    sort=[("created_at", -1)]  # Sort by newest first
                )
                
            except Exception as e:
                log.error(f"Database error during users retrieval: {str(e)}")
                error_detail = ErrorDetail(
                    code="DATABASE_ERROR",
                    message="Database connection error",
                    field="system"
                )
                return RestErrors.internal_server_error_500(
                    message="Database connection error",
                    data=None,
                    errors=[error_detail]
                )
            
            # Process and validate user data
            try:
                processed_users = []
                
                for user_data in users_data:
                    # Validate user data with User model
                    try:
                        del user_data['_id']
                        user = User(**user_data)
                    except Exception as e:
                        log.warning(f"User model validation failed for user {user_data.get('user_id', 'unknown')}: {str(e)}")
                        continue  # Skip invalid users instead of failing the entire request
                    
                    # Prepare response data (exclude sensitive information)
                    response_user_data = user_data.copy()
                    
                    # Convert ObjectId to string if present
                    if "_id" in response_user_data:
                        del response_user_data["_id"]
                    
                    # Remove sensitive fields from response
                    if response_user_data.get("security") and response_user_data["security"].get("password_hash"):
                        del response_user_data["security"]["password_hash"]
                    
                    if response_user_data.get("security") and response_user_data["security"].get("recovery_codes"):
                        del response_user_data["security"]["recovery_codes"]
                    
                    processed_users.append(response_user_data)
                
            except Exception as e:
                log.error(f"User data processing error: {str(e)}")
                error_detail = ErrorDetail(
                    code="DATA_PROCESSING_ERROR",
                    message="User data processing failed",
                    field="user_data"
                )
                return RestErrors.internal_server_error_500(
                    message="User data processing failed",
                    data=None,
                    errors=[error_detail]
                )
            
            # Prepare pagination metadata
            pagination_metadata = {
                "total_count": total_count,
                "returned_count": len(processed_users),
                "limit": limit,
                "skip": skip,
                "has_more": (skip + len(processed_users)) < total_count
            }
            
            # Prepare final response data
            response_data = {
                "users": processed_users,
                "pagination": pagination_metadata,
                "organization": {
                    "org_id": org_id,
                    "name": self.org_data.name
                }
            }
            
            log.info(f"Users retrieved successfully: {len(processed_users)} users from organization {org_id}")
            
            return RestErrors.success_200(
                message=f"Users retrieved successfully. Found {len(processed_users)} users.",
                data=response_data
            )
            
        except Exception as e:
            log.error(f"Unexpected error during users retrieval: {str(e)}")
            error_detail = ErrorDetail(
                code="UNEXPECTED_ERROR",
                message="An unexpected error occurred during users retrieval",
                field="system"
            )
            return RestErrors.internal_server_error_500(
                message="An unexpected error occurred during users retrieval",
                data=None,
                errors=[error_detail]
            )

    def get_user(self, current_user: UserProfile, user_id: str):
        """
        Retrieve a user by their user ID with comprehensive validation and error handling.
        
        Args:
            user_id (str): The unique identifier for the user
            
        Returns:
            dict: Response with success/error status and user data
        """
        try:
            # Input validation
            # Input validation
            org_id = current_user.org_id
            self._load_organization_data(org_id)
            
            if not self.org_data or self.org_data.status != 'active':
                log.warning(f"Get users failed: Invalid or inactive organization {org_id}")
                error_detail = ErrorDetail(
                    code="INVALID_ORGANIZATION",
                    message="Invalid or inactive organization",
                    field="org_id"
                )
                return RestErrors.bad_request_400(
                    message="Invalid or inactive organization",
                    data={"org_id": org_id},
                    errors=[error_detail]
                )
            if not user_id or not user_id.strip():
                error_detail = ErrorDetail(
                    code="MISSING_USER_ID",
                    message="User ID is required",
                    field="user_id"
                )
                return RestErrors.bad_request_400(
                    message="User ID is required",
                    data=None,
                    errors=[error_detail]
                )
            
            user_id = user_id.strip()
            log.info(f"Retrieving user with ID: {user_id}")
            
            # Query user from database
            try:
                user_data = self.mongo_client.find_one("users", {"user_id": user_id})
            except Exception as e:
                log.error(f"Database error during user retrieval: {str(e)}")
                error_detail = ErrorDetail(
                    code="DATABASE_ERROR",
                    message="Database connection error",
                    field="system"
                )
                return RestErrors.internal_server_error_500(
                    message="Database connection error",
                    data=None,
                    errors=[error_detail]
                )
            
            # Check if user exists
            if not user_data:
                log.warning(f"User not found with ID: {user_id}")
                error_detail = ErrorDetail(
                    code="USER_NOT_FOUND",
                    message="User not found",
                    field="user_id"
                )
                return RestErrors.not_found_404(
                    message="User not found",
                    data=None,
                    errors=[error_detail]
                )
            
            # Validate user data with User model
            try:
                del user_data['_id']
                user = User(**user_data)
            except Exception as e:
                log.error(f"User model validation error: {str(e)}")
                error_detail = ErrorDetail(
                    code="USER_MODEL_ERROR",
                    message="User data validation failed",
                    field="user_data"
                )
                return RestErrors.internal_server_error_500(
                    message="User data validation failed",
                    data=None,
                    errors=[error_detail]
                )
            
            # Prepare response data (exclude sensitive information)
            try:
                response_user_data = user_data.copy()
                
                # Convert ObjectId to string if present
                if "_id" in response_user_data:
                    del response_user_data["_id"]
                
                # Remove sensitive fields from response
                if response_user_data.get("security") and response_user_data["security"].get("password_hash"):
                    del response_user_data["security"]["password_hash"]
                
                if response_user_data.get("security") and response_user_data["security"].get("recovery_codes"):
                    del response_user_data["security"]["recovery_codes"]
                
            except Exception as e:
                log.error(f"Response preparation error: {str(e)}")
                error_detail = ErrorDetail(
                    code="RESPONSE_PREPARATION_ERROR",
                    message="Response preparation failed",
                    field="response"
                )
                return RestErrors.internal_server_error_500(
                    message="Response preparation failed",
                    data=None,
                    errors=[error_detail]
                )
            
            log.info(f"User retrieved successfully: {user_id}")
            
            return RestErrors.success_200(
                message="User retrieved successfully",
                data=response_user_data
            )
            
        except Exception as e:
            log.error(f"Unexpected error during user retrieval: {str(e)}")
            error_detail = ErrorDetail(
                code="UNEXPECTED_ERROR",
                message="An unexpected error occurred during user retrieval",
                field="system"
            )
            return RestErrors.internal_server_error_500(
                message="An unexpected error occurred during user retrieval",
                data=None,
                errors=[error_detail]
            )

    def create_user(self, current_user: UserProfile, user: User): 
        """
        Register a new user with comprehensive validation and error handling.
        
        Args:
            user (User): User registration data 
            
        Returns:
            dict: Response with success/error status and user data
        """
        try:
            # Extract required fields from User model structure
            # Input validation
            org_id = current_user.org_id
            self._load_organization_data(org_id)
            
            if not self.org_data or self.org_data.status != 'active':
                log.warning(f"Get users failed: Invalid or inactive organization {org_id}")
                error_detail = ErrorDetail(
                    code="INVALID_ORGANIZATION",
                    message="Invalid or inactive organization",
                    field="org_id"
                )
                return RestErrors.bad_request_400(
                    message="Invalid or inactive organization",
                    data={"org_id": org_id},
                    errors=[error_detail]
                )
            email = user.email.lower().strip() if user.email else None
            username = user.username.strip() if user.username else None
            
            # Extract password from security section or as direct attribute
            password = None
            if user.security and hasattr(user.security, 'password_hash') and user.security.password_hash:
                password = user.security.password_hash
            elif hasattr(user, 'password') and user.password:
                password = user.password
            
            # Extract first_name and last_name from profile section
            first_name = None
            last_name = None
            if user.profile:
                first_name = user.profile.first_name.strip() if user.profile.first_name else None
                last_name = user.profile.last_name.strip() if user.profile.last_name else None
            
            # Input validation - only email, username, and password are required
            if not email or not password or not username:
                error_detail = ErrorDetail(
                    code="MISSING_REQUIRED_FIELDS",
                    message="Email, password, and username are required",
                    field="email,password,username"
                )
                return RestErrors.bad_request_400(
                    message="Missing required fields",
                    data=None,
                    errors=[error_detail]
                )
            
            log.info(f"Registration attempt for email: {email}, username: {username}")
            
            # Check if email already exists
            try:
                existing_email_user = self.mongo_client.find_one("users", {"email": email})
            except Exception as e:
                log.error(f"Database error during email check: {str(e)}")
                error_detail = ErrorDetail(
                    code="DATABASE_ERROR",
                    message="Database connection error",
                    field="system"
                )
                return RestErrors.internal_server_error_500(
                    message="Database connection error",
                    data=None,
                    errors=[error_detail]
                )
            
            if existing_email_user:
                log.warning(f"Email already exists: {email}")
                error_detail = ErrorDetail(
                    code="EMAIL_ALREADY_EXISTS",
                    message="Email address is already registered",
                    field="email"
                )
                return RestErrors.bad_request_400(
                    message="Email address is already registered",
                    data=None,
                    errors=[error_detail]
                )
            
            # Check if username already exists
            try:
                existing_username_user = self.mongo_client.find_one("users", {"username": username})
            except Exception as e:
                log.error(f"Database error during username check: {str(e)}")
                error_detail = ErrorDetail(
                    code="DATABASE_ERROR",
                    message="Database connection error",
                    field="system"
                )
                return RestErrors.internal_server_error_500(
                    message="Database connection error",
                    data=None,
                    errors=[error_detail]
                )
            
            if existing_username_user:
                log.warning(f"Username already exists: {username}")
                error_detail = ErrorDetail(
                    code="USERNAME_ALREADY_EXISTS",
                    message="Username is already taken",
                    field="username"
                )
                return RestErrors.bad_request_400(
                    message="Username is already taken",
                    data=None,
                    errors=[error_detail]
                )
            
            # Encrypt password
            try:
                password_hash = Commons.get_encrypted_password(password)
            except Exception as e:
                log.error(f"Password encryption error: {str(e)}")
                error_detail = ErrorDetail(
                    code="PASSWORD_ENCRYPTION_ERROR",
                    message="Password encryption failed",
                    field="password"
                )
                return RestErrors.internal_server_error_500(
                    message="Password encryption failed",
                    data=None,
                    errors=[error_detail]
                )
            
            # Generate unique user ID
            user_id = str(uuid.uuid4())
            
            # Get current timestamp
            try:
                current_timestamp = Commons.get_timestamp_in_utc()
            except Exception as e:
                log.error(f"Timestamp generation error: {str(e)}")
                error_detail = ErrorDetail(
                    code="TIMESTAMP_ERROR",
                    message="Timestamp generation failed",
                    field="timestamp"
                )
                return RestErrors.internal_server_error_500(
                    message="Timestamp generation failed",
                    data=None,
                    errors=[error_detail]
                )
            
            # Create comprehensive user data with all hierarchical parameters and default values
            try:
                user_data = {
                    "user_id": user_id,
                    "email": email,
                    "username": username,
                    
                    # Profile section with defaults
                    "profile": {
                        "first_name": first_name if first_name else "",
                        "last_name": last_name if last_name else "",
                        "bio": user.profile.bio if user.profile and user.profile.bio else None,
                        "date_of_birth": user.profile.date_of_birth if user.profile and user.profile.date_of_birth else None,
                        "profile_picture_url": user.profile.profile_picture_url if user.profile and user.profile.profile_picture_url else None,
                        "phone_number": user.profile.phone_number if user.profile and user.profile.phone_number else None,
                        "gender": user.profile.gender if user.profile and user.profile.gender else None,
                        "locale": user.profile.locale if user.profile and user.profile.locale else "en-US",
                        "timezone": user.profile.timezone if user.profile and user.profile.timezone else None
                    },
                    
                    # Address section with defaults - always include structure
                    "address": {
                        "street": user.address.street if user.address and user.address.street else None,
                        "city": user.address.city if user.address and user.address.city else None,
                        "state": user.address.state if user.address and user.address.state else None,
                        "postal_code": user.address.postal_code if user.address and user.address.postal_code else None,
                        "country": user.address.country if user.address and user.address.country else None
                    },
                    
                    # Preferences section with defaults
                    "preferences": {
                        "theme": user.preferences.theme if user.preferences and user.preferences.theme else "light",
                        "notifications_enabled": user.preferences.notifications_enabled if user.preferences and hasattr(user.preferences, 'notifications_enabled') and user.preferences.notifications_enabled is not None else True,
                        "email_notifications_enabled": user.preferences.email_notifications_enabled if user.preferences and hasattr(user.preferences, 'email_notifications_enabled') and user.preferences.email_notifications_enabled is not None else True,
                        "is_public": user.preferences.is_public if user.preferences and hasattr(user.preferences, 'is_public') and user.preferences.is_public is not None else True,
                        "content_language": user.preferences.content_language if user.preferences and user.preferences.content_language else "en"
                    },
                    
                    # Security section with defaults
                    "security": {
                        "is_email_verified": user.security.is_email_verified if user.security and hasattr(user.security, 'is_email_verified') and user.security.is_email_verified is not None else False,
                        "is_phone_verified": user.security.is_phone_verified if user.security and hasattr(user.security, 'is_phone_verified') and user.security.is_phone_verified is not None else False,
                        "password_hash": password_hash,
                        "last_login": user.security.last_login if user.security and user.security.last_login else None,
                        "mfa_enabled": user.security.mfa_enabled if user.security and hasattr(user.security, 'mfa_enabled') and user.security.mfa_enabled is not None else False,
                        "recovery_codes": user.security.recovery_codes if user.security and user.security.recovery_codes else []
                    },
                    
                    # Organization section with defaults - always include structure
                    "organization": {
                        "org_id": user.organization.org_id if user.organization and user.organization.org_id else None,
                        "name": user.organization.name if user.organization and user.organization.name else None
                    },
                    
                    # Business units section with defaults - always include structure
                    "business_units": [
                        {
                            "bu_id": bu.bu_id if bu.bu_id else None,
                            "name": bu.name if bu.name else None
                        } for bu in user.business_units
                    ] if user.business_units else [],
                    
                    # Membership section with defaults
                    "membership": {
                        "status": user.membership.status if user.membership and user.membership.status else "free_tier",
                        "start_date": user.membership.start_date if user.membership and user.membership.start_date else current_timestamp,
                        "end_date": user.membership.end_date if user.membership and user.membership.end_date else None
                    },
                    
                    # Social profiles section with defaults
                    "social_profiles": [
                        {
                            "platform": profile.platform if profile.platform else "",
                            "url": str(profile.url) if profile.url else "",
                            "handle": profile.handle if profile.handle else ""
                        } for profile in user.social_profiles
                    ] if user.social_profiles else [],
                    
                    # Lists with defaults
                    "roles": user.roles if user.roles else ["user"],
                    "groups": user.groups if user.groups else [],
                    "tags": user.tags if user.tags else ["new_user"],
                    
                    # Metadata with defaults - always include structure
                    "metadata": {
                        "registration_ip": user.metadata.registration_ip if user.metadata and hasattr(user.metadata, 'registration_ip') else None,
                        "registration_source": user.metadata.registration_source if user.metadata and hasattr(user.metadata, 'registration_source') else "web",
                        "last_activity": user.metadata.last_activity if user.metadata and hasattr(user.metadata, 'last_activity') else current_timestamp,
                        "user_agent": user.metadata.user_agent if user.metadata and hasattr(user.metadata, 'user_agent') else None,
                        "referral_source": user.metadata.referral_source if user.metadata and hasattr(user.metadata, 'referral_source') else None
                    },
                    
                    # Timestamps
                    "created_at": user.created_at if user.created_at else current_timestamp,
                    "updated_at": current_timestamp,
                    
                    # Status flags with defaults
                    "is_active": user.is_active if hasattr(user, 'is_active') and user.is_active is not None else True,
                    "is_banned": user.is_banned if hasattr(user, 'is_banned') and user.is_banned is not None else False,
                    "is_suspended": user.is_suspended if hasattr(user, 'is_suspended') and user.is_suspended is not None else False,
                    "is_logged_in": user.is_logged_in if hasattr(user, 'is_logged_in') and user.is_logged_in is not None else False
                }
                
                # Validate user data with User model
                validated_user = User(**user_data)
                
            except Exception as e:
                log.error(f"User model creation error: {str(e)}")
                error_detail = ErrorDetail(
                    code="USER_MODEL_ERROR",
                    message="User data validation failed",
                    field="user_data"
                )
                return RestErrors.internal_server_error_500(
                    message="User data validation failed",
                    data=None,
                    errors=[error_detail]
                )
            
            # Insert user data into MongoDB
            try:
                insert_result = self.mongo_client.insert_data("users", user_data, document_id=user_id)
                
                if not insert_result:
                    log.error(f"Failed to insert user data for: {email}")
                    error_detail = ErrorDetail(
                        code="DATABASE_INSERT_FAILED",
                        message="Failed to create user account",
                        field="database"
                    )
                    return RestErrors.internal_server_error_500(
                        message="Failed to create user account",
                        data=None,
                        errors=[error_detail]
                    )
                
                log.info(f"User registered successfully: {email}")
                
            except Exception as e:
                log.error(f"Database insert error: {str(e)}")
                error_detail = ErrorDetail(
                    code="DATABASE_INSERT_ERROR",
                    message="Database insert operation failed",
                    field="database"
                )
                return RestErrors.internal_server_error_500(
                    message="Database insert operation failed",
                    data=None,
                    errors=[error_detail]
                )
            
            # Prepare response data (exclude password_hash and convert ObjectId)
            try:
                response_user_data = user_data.copy()
                
                # Convert ObjectId to string if present (MongoDB adds _id field)
                if "_id" in response_user_data:
                    del response_user_data["_id"]
                
                # Remove sensitive fields from response
                if response_user_data.get("security") and response_user_data["security"].get("password_hash"):
                    del response_user_data["security"]["password_hash"]
                
                if response_user_data.get("security") and response_user_data["security"].get("recovery_codes"):
                    del response_user_data["security"]["recovery_codes"]
                
            except Exception as e:
                log.error(f"Response preparation error: {str(e)}")
                error_detail = ErrorDetail(
                    code="RESPONSE_PREPARATION_ERROR",
                    message="Response preparation failed",
                    field="response"
                )
                return RestErrors.internal_server_error_500(
                    message="Response preparation failed",
                    data=None,
                    errors=[error_detail]
                )
            
            log.info(f"User registration completed successfully for: {email}")
            
            return RestErrors.created_201(
                message="User registered successfully",
                data=response_user_data
            )
            
        except Exception as e:
            log.error(f"Unexpected error during registration: {str(e)}")
            error_detail = ErrorDetail(
                code="UNEXPECTED_ERROR",
                message="An unexpected error occurred during registration",
                field="system"
            )
            return RestErrors.internal_server_error_500(
                message="An unexpected error occurred during registration",
                data=None,
                errors=[error_detail]
            )

    def update_user(self, current_user: UserProfile, user: User, user_id: str):
        """
        Update an existing user with comprehensive validation and error handling.
        Only updates fields that are provided and valid in the request.
        
        Args:
            current_user (UserProfile): The authenticated user making the request
            user (User): User data with fields to update
            user_id (str): The unique identifier for the user to update
            
        Returns:
            dict: Response with success/error status and updated user data
        """
        try:
            # Input validation - Organization validation
            org_id = current_user.org_id
            self._load_organization_data(org_id)
            
            if not self.org_data or self.org_data.status != 'active':
                log.warning(f"Update user failed: Invalid or inactive organization {org_id}")
                error_detail = ErrorDetail(
                    code="INVALID_ORGANIZATION",
                    message="Invalid or inactive organization",
                    field="org_id"
                )
                return RestErrors.bad_request_400(
                    message="Invalid or inactive organization",
                    data={"org_id": org_id},
                    errors=[error_detail]
                )
            
            # Validate user_id
            if not user_id or not user_id.strip():
                error_detail = ErrorDetail(
                    code="MISSING_USER_ID",
                    message="User ID is required",
                    field="user_id"
                )
                return RestErrors.bad_request_400(
                    message="User ID is required",
                    data=None,
                    errors=[error_detail]
                )
            
            user_id = user_id.strip()
            log.info(f"Updating user with ID: {user_id}")
            
            # Get existing user from database
            try:
                existing_user_data = self.mongo_client.find_one("users", {"user_id": user_id})
            except Exception as e:
                log.error(f"Database error during user retrieval: {str(e)}")
                error_detail = ErrorDetail(
                    code="DATABASE_ERROR",
                    message="Database connection error",
                    field="system"
                )
                return RestErrors.internal_server_error_500(
                    message="Database connection error",
                    data=None,
                    errors=[error_detail]
                )
            
            # Check if user exists
            if not existing_user_data:
                log.warning(f"User not found with ID: {user_id}")
                error_detail = ErrorDetail(
                    code="USER_NOT_FOUND",
                    message="User not found",
                    field="user_id"
                )
                return RestErrors.not_found_404(
                    message="User not found",
                    data=None,
                    errors=[error_detail]
                )
            
            # Prepare update data by checking each field in the hierarchy
            update_data = {}
            invalid_fields = []
            
            # Define valid top-level fields from User model
            valid_top_level_fields = {
                'user_id', 'email', 'username', 'profile', 'address', 'preferences', 
                'security', 'org_id', 'business_units', 'membership', 
                'social_profiles', 'roles', 'groups', 'tags', 'metadata', 
                'created_at', 'updated_at', 'is_active', 'is_banned', 
                'is_suspended', 'is_logged_in'
            }
            
            # Check for invalid top-level fields in the request
            user_dict = user.dict(exclude_unset=True)
            for field_name in user_dict.keys():
                if field_name not in valid_top_level_fields:
                    invalid_fields.append(field_name)
            
            # If there are invalid top-level fields, return error immediately
            if invalid_fields:
                log.warning(f"Invalid top-level fields provided for user update: {invalid_fields}")
                error_details = []
                for field in invalid_fields:
                    error_details.append(ErrorDetail(
                        code="INVALID_FIELD",
                        message=f"Field '{field}' is not a valid top-level field. Did you mean 'security.{field}' or another nested field?",
                        field=field
                    ))
                return RestErrors.bad_request_400(
                    message=f"Invalid top-level fields provided: {', '.join(invalid_fields)}",
                    data=None,
                    errors=error_details
                )
            
            # Top-level fields validation
            if user.email is not None:
                if 'email' not in existing_user_data:
                    invalid_fields.append('email')
                else:
                    email = user.email.lower().strip()
                    # Check if email is already taken by another user
                    try:
                        existing_email_user = self.mongo_client.find_one("users", {"email": email, "user_id": {"$ne": user_id}})
                        if existing_email_user:
                            error_detail = ErrorDetail(
                                code="EMAIL_ALREADY_EXISTS",
                                message="Email address is already registered by another user",
                                field="email"
                            )
                            return RestErrors.bad_request_400(
                                message="Email address is already registered by another user",
                                data=None,
                                errors=[error_detail]
                            )
                        update_data["email"] = email
                    except Exception as e:
                        log.error(f"Database error during email check: {str(e)}")
                        error_detail = ErrorDetail(
                            code="DATABASE_ERROR",
                            message="Database connection error during email validation",
                            field="system"
                        )
                        return RestErrors.internal_server_error_500(
                            message="Database connection error during email validation",
                            data=None,
                            errors=[error_detail]
                        )
            
            if user.username is not None:
                if 'username' not in existing_user_data:
                    invalid_fields.append('username')
                else:
                    username = user.username.strip()
                    # Check if username is already taken by another user
                    try:
                        existing_username_user = self.mongo_client.find_one("users", {"username": username, "user_id": {"$ne": user_id}})
                        if existing_username_user:
                            error_detail = ErrorDetail(
                                code="USERNAME_ALREADY_EXISTS",
                                message="Username is already taken by another user",
                                field="username"
                            )
                            return RestErrors.bad_request_400(
                                message="Username is already taken by another user",
                                data=None,
                                errors=[error_detail]
                            )
                        update_data["username"] = username
                    except Exception as e:
                        log.error(f"Database error during username check: {str(e)}")
                        error_detail = ErrorDetail(
                            code="DATABASE_ERROR",
                            message="Database connection error during username validation",
                            field="system"
                        )
                        return RestErrors.internal_server_error_500(
                            message="Database connection error during username validation",
                            data=None,
                            errors=[error_detail]
                        )
            
            if user.roles is not None:
                if 'roles' not in existing_user_data:
                    invalid_fields.append('roles')
                else:
                    update_data["roles"] = user.roles
            
            if user.groups is not None:
                if 'groups' not in existing_user_data:
                    invalid_fields.append('groups')
                else:
                    update_data["groups"] = user.groups
            
            if user.tags is not None:
                if 'tags' not in existing_user_data:
                    invalid_fields.append('tags')
                else:
                    update_data["tags"] = user.tags
            
            if user.metadata is not None:
                if 'metadata' not in existing_user_data:
                    invalid_fields.append('metadata')
                else:
                    update_data["metadata"] = user.metadata
            
            if user.is_active is not None:
                if 'is_active' not in existing_user_data:
                    invalid_fields.append('is_active')
                else:
                    update_data["is_active"] = user.is_active
            
            if user.is_banned is not None:
                if 'is_banned' not in existing_user_data:
                    invalid_fields.append('is_banned')
                else:
                    update_data["is_banned"] = user.is_banned
            
            if user.is_suspended is not None:
                if 'is_suspended' not in existing_user_data:
                    invalid_fields.append('is_suspended')
                else:
                    update_data["is_suspended"] = user.is_suspended
            
            if user.is_logged_in is not None:
                if 'is_logged_in' not in existing_user_data:
                    invalid_fields.append('is_logged_in')
                else:
                    update_data["is_logged_in"] = user.is_logged_in
            
            # Profile nested object validation
            if user.profile is not None:
                if 'profile' not in existing_user_data:
                    invalid_fields.append('profile')
                else:
                    profile_updates = {}
                    existing_profile = existing_user_data.get('profile', {})
                    
                    if user.profile.first_name is not None:
                        if 'first_name' not in existing_profile:
                            invalid_fields.append('profile.first_name')
                        else:
                            profile_updates["profile.first_name"] = user.profile.first_name.strip()
                    
                    if user.profile.last_name is not None:
                        if 'last_name' not in existing_profile:
                            invalid_fields.append('profile.last_name')
                        else:
                            profile_updates["profile.last_name"] = user.profile.last_name.strip()
                    
                    if user.profile.bio is not None:
                        if 'bio' not in existing_profile:
                            invalid_fields.append('profile.bio')
                        else:
                            profile_updates["profile.bio"] = user.profile.bio
                    
                    if user.profile.date_of_birth is not None:
                        if 'date_of_birth' not in existing_profile:
                            invalid_fields.append('profile.date_of_birth')
                        else:
                            profile_updates["profile.date_of_birth"] = user.profile.date_of_birth
                    
                    if user.profile.profile_picture_url is not None:
                        if 'profile_picture_url' not in existing_profile:
                            invalid_fields.append('profile.profile_picture_url')
                        else:
                            profile_updates["profile.profile_picture_url"] = str(user.profile.profile_picture_url)
                    
                    if user.profile.phone_number is not None:
                        if 'phone_number' not in existing_profile:
                            invalid_fields.append('profile.phone_number')
                        else:
                            profile_updates["profile.phone_number"] = user.profile.phone_number
                    
                    if user.profile.gender is not None:
                        if 'gender' not in existing_profile:
                            invalid_fields.append('profile.gender')
                        else:
                            profile_updates["profile.gender"] = user.profile.gender
                    
                    if user.profile.locale is not None:
                        if 'locale' not in existing_profile:
                            invalid_fields.append('profile.locale')
                        else:
                            profile_updates["profile.locale"] = user.profile.locale
                    
                    if user.profile.timezone is not None:
                        if 'timezone' not in existing_profile:
                            invalid_fields.append('profile.timezone')
                        else:
                            profile_updates["profile.timezone"] = user.profile.timezone
                    
                    update_data.update(profile_updates)
            
            # Address nested object validation
            if user.address is not None:
                if 'address' not in existing_user_data:
                    invalid_fields.append('address')
                else:
                    address_updates = {}
                    existing_address = existing_user_data.get('address', {})
                    
                    if user.address.street is not None:
                        if 'street' not in existing_address:
                            invalid_fields.append('address.street')
                        else:
                            address_updates["address.street"] = user.address.street
                    
                    if user.address.city is not None:
                        if 'city' not in existing_address:
                            invalid_fields.append('address.city')
                        else:
                            address_updates["address.city"] = user.address.city
                    
                    if user.address.state is not None:
                        if 'state' not in existing_address:
                            invalid_fields.append('address.state')
                        else:
                            address_updates["address.state"] = user.address.state
                    
                    if user.address.postal_code is not None:
                        if 'postal_code' not in existing_address:
                            invalid_fields.append('address.postal_code')
                        else:
                            address_updates["address.postal_code"] = user.address.postal_code
                    
                    if user.address.country is not None:
                        if 'country' not in existing_address:
                            invalid_fields.append('address.country')
                        else:
                            address_updates["address.country"] = user.address.country
                    
                    update_data.update(address_updates)
            
            # Preferences nested object validation
            if user.preferences is not None:
                if 'preferences' not in existing_user_data:
                    invalid_fields.append('preferences')
                else:
                    preferences_updates = {}
                    existing_preferences = existing_user_data.get('preferences', {})
                    
                    if user.preferences.theme is not None:
                        if 'theme' not in existing_preferences:
                            invalid_fields.append('preferences.theme')
                        else:
                            preferences_updates["preferences.theme"] = user.preferences.theme
                    
                    if user.preferences.notifications_enabled is not None:
                        if 'notifications_enabled' not in existing_preferences:
                            invalid_fields.append('preferences.notifications_enabled')
                        else:
                            preferences_updates["preferences.notifications_enabled"] = user.preferences.notifications_enabled
                    
                    if user.preferences.email_notifications_enabled is not None:
                        if 'email_notifications_enabled' not in existing_preferences:
                            invalid_fields.append('preferences.email_notifications_enabled')
                        else:
                            preferences_updates["preferences.email_notifications_enabled"] = user.preferences.email_notifications_enabled
                    
                    if user.preferences.is_public is not None:
                        if 'is_public' not in existing_preferences:
                            invalid_fields.append('preferences.is_public')
                        else:
                            preferences_updates["preferences.is_public"] = user.preferences.is_public
                    
                    if user.preferences.content_language is not None:
                        if 'content_language' not in existing_preferences:
                            invalid_fields.append('preferences.content_language')
                        else:
                            preferences_updates["preferences.content_language"] = user.preferences.content_language
                    
                    update_data.update(preferences_updates)
            
            # Security nested object validation (excluding password_hash and recovery_codes for security)
            if user.security is not None:
                if 'security' not in existing_user_data:
                    invalid_fields.append('security')
                else:
                    security_updates = {}
                    existing_security = existing_user_data.get('security', {})
                    
                    if user.security.is_email_verified is not None:
                        if 'is_email_verified' not in existing_security:
                            invalid_fields.append('security.is_email_verified')
                        else:
                            security_updates["security.is_email_verified"] = user.security.is_email_verified
                    
                    if user.security.is_phone_verified is not None:
                        if 'is_phone_verified' not in existing_security:
                            invalid_fields.append('security.is_phone_verified')
                        else:
                            security_updates["security.is_phone_verified"] = user.security.is_phone_verified
                    
                    if user.security.mfa_enabled is not None:
                        if 'mfa_enabled' not in existing_security:
                            invalid_fields.append('security.mfa_enabled')
                        else:
                            security_updates["security.mfa_enabled"] = user.security.mfa_enabled
                    
                    if user.security.last_login is not None:
                        if 'last_login' not in existing_security:
                            invalid_fields.append('security.last_login')
                        else:
                            security_updates["security.last_login"] = user.security.last_login
                    
                    update_data.update(security_updates)
            
            # Organization nested object validation
            if user.org_id is not None:
                if 'org_id' not in existing_user_data:
                    invalid_fields.append('org_id')
                else:
                    update_data["org_id"] = user.org_id

            
            # Business units array validation
            if user.business_units is not None:
                if 'business_units' not in existing_user_data:
                    invalid_fields.append('business_units')
                else:
                    update_data["business_units"] = user.business_units
            
            # Membership nested object validation
            if user.membership is not None:
                if 'membership' not in existing_user_data:
                    invalid_fields.append('membership')
                else:
                    membership_updates = {}
                    existing_membership = existing_user_data.get('membership', {})
                    
                    if user.membership.status is not None:
                        if 'status' not in existing_membership:
                            invalid_fields.append('membership.status')
                        else:
                            membership_updates["membership.status"] = user.membership.status
                    
                    if user.membership.start_date is not None:
                        if 'start_date' not in existing_membership:
                            invalid_fields.append('membership.start_date')
                        else:
                            membership_updates["membership.start_date"] = user.membership.start_date
                    
                    if user.membership.end_date is not None:
                        if 'end_date' not in existing_membership:
                            invalid_fields.append('membership.end_date')
                        else:
                            membership_updates["membership.end_date"] = user.membership.end_date
                    
                    update_data.update(membership_updates)
            
            # Social profiles array validation
            if user.social_profiles is not None:
                if 'social_profiles' not in existing_user_data:
                    invalid_fields.append('social_profiles')
                else:
                    social_profiles_data = []
                    for sp in user.social_profiles:
                        sp_data = {}
                        if sp.platform is not None:
                            sp_data["platform"] = sp.platform
                        if sp.url is not None:
                            sp_data["url"] = str(sp.url)
                        if sp.handle is not None:
                            sp_data["handle"] = sp.handle
                        social_profiles_data.append(sp_data)
                    update_data["social_profiles"] = social_profiles_data
            
            # Check if there are invalid fields
            if invalid_fields:
                log.warning(f"Invalid fields provided for user update: {invalid_fields}")
                error_details = []
                for field in invalid_fields:
                    error_details.append(ErrorDetail(
                        code="INVALID_FIELD",
                        message=f"Field '{field}' does not exist in user data structure",
                        field=field
                    ))
                return RestErrors.bad_request_400(
                    message=f"Invalid fields provided: {', '.join(invalid_fields)}",
                    data=None,
                    errors=error_details
                )
            
            # Always update the updated_at timestamp
            try:
                update_data["updated_at"] = Commons.get_timestamp_in_utc()
            except Exception as e:
                log.error(f"Timestamp generation error: {str(e)}")
                error_detail = ErrorDetail(
                    code="TIMESTAMP_ERROR",
                    message="Timestamp generation failed",
                    field="timestamp"
                )
                return RestErrors.internal_server_error_500(
                    message="Timestamp generation failed",
                    data=None,
                    errors=[error_detail]
                )
            
            # Check if there are any fields to update
            if not update_data or len(update_data) == 1:  # Only updated_at
                log.warning(f"No valid fields provided for update for user: {user_id}")
                error_detail = ErrorDetail(
                    code="NO_FIELDS_TO_UPDATE",
                    message="No valid fields provided for update",
                    field="user_data"
                )
                return RestErrors.bad_request_400(
                    message="No valid fields provided for update",
                    data=None,
                    errors=[error_detail]
                )
            
            # Update user in database
            try:
                result = self.mongo_client.update_data(
                    collection_name="users",
                    filter_dict={"user_id": user_id},
                    update_dict={"$set": update_data},
                    upsert=False,
                    update_many=False
                )
                
                if not result:
                    log.warning(f"No changes made to user: {user_id}")
                    error_detail = ErrorDetail(
                        code="NO_CHANGES_MADE",
                        message="No changes were made to the user",
                        field="user_data"
                    )
                    return RestErrors.bad_request_400(
                        message="No changes were made to the user",
                        data=None,
                        errors=[error_detail]
                    )
                
            except Exception as e:
                log.error(f"Database error during user update: {str(e)}")
                error_detail = ErrorDetail(
                    code="DATABASE_ERROR",
                    message="Database update error",
                    field="system"
                )
                return RestErrors.internal_server_error_500(
                    message="Database update error",
                    data=None,
                    errors=[error_detail]
                )
            
            # Retrieve updated user data
            try:
                updated_user_data = self.mongo_client.find_one("users", {"user_id": user_id})
            except Exception as e:
                log.error(f"Database error during updated user retrieval: {str(e)}")
                error_detail = ErrorDetail(
                    code="DATABASE_ERROR",
                    message="Database retrieval error",
                    field="system"
                )
                return RestErrors.internal_server_error_500(
                    message="Database retrieval error",
                    data=None,
                    errors=[error_detail]
                )
            
            # Prepare response data (exclude sensitive information)
            try:
                response_user_data = updated_user_data.copy()
                
                # Convert ObjectId to string if present
                if "_id" in response_user_data:
                    del response_user_data["_id"]
                
                # Remove sensitive fields from response
                if response_user_data.get("security") and response_user_data["security"].get("password_hash"):
                    del response_user_data["security"]["password_hash"]
                
                if response_user_data.get("security") and response_user_data["security"].get("recovery_codes"):
                    del response_user_data["security"]["recovery_codes"]
                
            except Exception as e:
                log.error(f"Response preparation error: {str(e)}")
                error_detail = ErrorDetail(
                    code="RESPONSE_PREPARATION_ERROR",
                    message="Response preparation failed",
                    field="response"
                )
                return RestErrors.internal_server_error_500(
                    message="Response preparation failed",
                    data=None,
                    errors=[error_detail]
                )
            
            log.info(f"User updated successfully: {user_id}")
            
            return RestErrors.success_200(
                message="User updated successfully",
                data=response_user_data
            )
            
        except Exception as e:
            log.error(f"Unexpected error during user update: {str(e)}")
            error_detail = ErrorDetail(
                code="UNEXPECTED_ERROR",
                message="An unexpected error occurred during user update",
                field="system"
            )
            return RestErrors.internal_server_error_500(
                message="An unexpected error occurred during user update",
                data=None,
                errors=[error_detail]
            )

    def delete_user(self, current_user: UserProfile, user_id: str):
        """
        Delete a user by their user ID with comprehensive validation and error handling.
        This performs a hard delete, permanently removing the user from the database.
        
        Args:
            user_id (str): The unique identifier for the user to delete
            
        Returns:
            dict: Response with success/error status
        """
        try:
            # Input validation
            # Input validation
            org_id = current_user.org_id
            self._load_organization_data(org_id)
            
            if not self.org_data or self.org_data.status != 'active':
                log.warning(f"Get users failed: Invalid or inactive organization {org_id}")
                error_detail = ErrorDetail(
                    code="INVALID_ORGANIZATION",
                    message="Invalid or inactive organization",
                    field="org_id"
                )
                return RestErrors.bad_request_400(
                    message="Invalid or inactive organization",
                    data={"org_id": org_id},
                    errors=[error_detail]
                )
            if not user_id or not user_id.strip():
                error_detail = ErrorDetail(
                    code="MISSING_USER_ID",
                    message="User ID is required",
                    field="user_id"
                )
                return RestErrors.bad_request_400(
                    message="User ID is required",
                    data=None,
                    errors=[error_detail]
                )
            
            user_id = user_id.strip()
            log.info(f"Attempting to delete user with ID: {user_id}")
            
            # Check if user exists before deletion
            try:
                existing_user = self.mongo_client.find_one("users", {"user_id": user_id})
            except Exception as e:
                log.error(f"Database error during user existence check: {str(e)}")
                error_detail = ErrorDetail(
                    code="DATABASE_ERROR",
                    message="Database connection error",
                    field="system"
                )
                return RestErrors.internal_server_error_500(
                    message="Database connection error",
                    data=None,
                    errors=[error_detail]
                )
            
            # Check if user exists
            if not existing_user:
                log.warning(f"User not found for deletion with ID: {user_id}")
                error_detail = ErrorDetail(
                    code="USER_NOT_FOUND",
                    message="User not found",
                    field="user_id"
                )
                return RestErrors.not_found_404(
                    message="User not found",
                    data=None,
                    errors=[error_detail]
                )
            
            # Validate existing user data with User model
            try:
                del existing_user['_id']
                user = User(**existing_user)
            except Exception as e:
                log.error(f"User model validation error during deletion: {str(e)}")
                error_detail = ErrorDetail(
                    code="USER_MODEL_ERROR",
                    message="User data validation failed",
                    field="user_data"
                )
                return RestErrors.internal_server_error_500(
                    message="User data validation failed",
                    data=None,
                    errors=[error_detail]
                )
            
            # Perform hard delete from database
            try:
                delete_result = self.mongo_client.delete_data("users", {"user_id": user_id})
                
                if not delete_result:
                    log.error(f"Failed to delete user from database: {user_id}")
                    error_detail = ErrorDetail(
                        code="DATABASE_DELETE_FAILED",
                        message="Failed to delete user from database",
                        field="database"
                    )
                    return RestErrors.internal_server_error_500(
                        message="Failed to delete user from database",
                        data=None,
                        errors=[error_detail]
                    )
                
                log.info(f"User deleted successfully from database: {user_id}")
                
            except Exception as e:
                log.error(f"Database delete error: {str(e)}")
                error_detail = ErrorDetail(
                    code="DATABASE_DELETE_ERROR",
                    message="Database delete operation failed",
                    field="database"
                )
                return RestErrors.internal_server_error_500(
                    message="Database delete operation failed",
                    data=None,
                    errors=[error_detail]
                )
            
            log.info(f"User deletion completed successfully for: {user_id}")
            
            # Return 204 No Content for successful deletion
            return RestErrors.no_content_204(
                message="User deleted successfully",
                data={"user_id": user_id}
            )
            
        except Exception as e:
            log.error(f"Unexpected error during user deletion: {str(e)}")
            error_detail = ErrorDetail(
                code="UNEXPECTED_ERROR",
                message="An unexpected error occurred during user deletion",
                field="system"
            )
            return RestErrors.internal_server_error_500(
                message="An unexpected error occurred during user deletion",
                data=None,
                errors=[error_detail]
            )