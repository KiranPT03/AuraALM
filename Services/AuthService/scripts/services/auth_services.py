import uuid

from scripts.utils.logger import log
from scripts.utils.rest_errors import RestErrors
from scripts.databases.mongodb.client import MongoClient
from scripts.models.request import LoginRequest
from scripts.services.jwt import JWTService
from scripts.utils.commons import Commons
from scripts.models.user import User
from scripts.models.response import AuthResponse, ErrorDetail


class AuthorizationService:
    def __init__(self, config):
        self.config = config
        mongo_config = self.config.get_mongodb_config()
        self.mongo_client = MongoClient(mongo_config)
        self.jwt_service = JWTService(config)

    def get_user_data(self, email: str):
        """
        Retrieve user data from MongoDB by email address.
        
        Args:
            email (str): User's email address
            
        Returns:
            dict: User data if found, None if not found
            
        Raises:
            Exception: Database connection or query errors
        """
        try:
            # Input validation
            if not email or not isinstance(email, str):
                log.error("Invalid email parameter provided to get_user_data")
                raise ValueError("Email must be a non-empty string")
            
            # Normalize email (convert to lowercase)
            email = email.lower().strip()
            
            # Validate email format (basic check)
            if '@' not in email or '.' not in email:
                log.warning(f"Invalid email format: {email}")
                raise ValueError("Invalid email format")
            
            log.info(f"Attempting to retrieve user data for email: {email}")
            
            # Query database
            user = self.mongo_client.find_one("users", {"email": email})
            
            if user:
                log.info(f"User data found for email: {email}")
                # Remove sensitive data from logs
                log.debug(f"User ID: {user.get('user_id', 'N/A')}")
            else:
                log.info(f"No user found for email: {email}")
            
            return user
            
        except ValueError as ve:
            log.error(f"Validation error in get_user_data: {str(ve)}")
            raise ve
        except Exception as e:
            log.error(f"Database error in get_user_data for email {email}: {str(e)}")
            raise Exception(f"Failed to retrieve user data: {str(e)}")

    def update_login_parametrs(self, email: str):
        """
        Update user login parameters including timestamps and login status.
        
        Args:
            email (str): User's email address
            
        Returns:
            tuple: (success: bool, error_detail: ErrorDetail or None)
        """
        try:
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
                return False, error_detail
            
            # Prepare update data for database
            update_data = {
                "$set": {
                    "security.last_login": current_timestamp,
                    "metadata.last_activity": current_timestamp,
                    "updated_at": current_timestamp,
                    "is_logged_in": True
                }
            }
            
            # Update user in database using the correct method
            try:
                update_result = self.mongo_client.update_data(
                    "users",
                    {"email": email},
                    update_data
                )
                
                if not update_result:
                    log.warning(f"Failed to update user login timestamp: {email}")
                    error_detail = ErrorDetail(
                        code="DATABASE_UPDATE_FAILED",
                        message="Failed to update user login parameters",
                        field="database"
                    )
                    return False, error_detail
                else:
                    log.info(f"User login timestamp updated successfully: {email}")
                    return True, None
                    
            except Exception as e:
                log.error(f"Error updating user login data: {str(e)}")
                error_detail = ErrorDetail(
                    code="DATABASE_UPDATE_ERROR",
                    message="Error updating user login data",
                    field="database"
                )
                return False, error_detail
                
        except Exception as e:
            log.error(f"Unexpected error in update_login_parametrs: {str(e)}")
            error_detail = ErrorDetail(
                code="UNEXPECTED_UPDATE_ERROR",
                message="Unexpected error during login parameter update",
                field="system"
            )
            return False, error_detail

    def login(self, login_request: LoginRequest):
        try:
            # Input validation
            if not login_request.email or not login_request.password:
                error_detail = ErrorDetail(
                    code="MISSING_CREDENTIALS",
                    message="Email and password are required",
                    field="email,password"
                )
                return RestErrors.bad_request_400(
                    message="Email and password are required",
                    data=None,
                    errors=[error_detail]
                )
            
            email = login_request.email
            password = login_request.password
            
            log.info(f"Login attempt for email: {email}")
            
            # Get user data from database with enhanced error handling
            try:
                user_data = self.get_user_data(email)
            except ValueError as ve:
                log.warning(f"Invalid email format in login: {str(ve)}")
                error_detail = ErrorDetail(
                    code="INVALID_EMAIL_FORMAT",
                    message="Invalid email format",
                    field="email"
                )
                return RestErrors.bad_request_400(
                    message="Invalid email format",
                    data=None,
                    errors=[error_detail]
                )
            except Exception as e:
                log.error(f"Database error during user lookup: {str(e)}")
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
            
            if not user_data:
                log.warning(f"User not found: {email}")
                error_detail = ErrorDetail(
                    code="INVALID_CREDENTIALS",
                    message="Invalid email or password",
                    field="email,password"
                )
                return RestErrors.unauthorized_401(
                    message="Invalid email or password",
                    data=None,
                    errors=[error_detail]
                )
            
            # Convert dict to User model for easier access
            try:
                user = User(**user_data)
            except Exception as e:
                log.error(f"Error parsing user data: {str(e)}")
                error_detail = ErrorDetail(
                    code="USER_DATA_FORMAT_ERROR",
                    message="User data format error",
                    field="user_data"
                )
                return RestErrors.internal_server_error_500(
                    message="User data format error",
                    data=None,
                    errors=[error_detail]
                )
            
            log.info(f"User data retrieved for: {email}")
            
            # Check if user account is active
            if not user.is_active:
                log.warning(f"Inactive account login attempt: {email}")
                error_detail = ErrorDetail(
                    code="ACCOUNT_INACTIVE",
                    message="Account is inactive. Please contact support.",
                    field="account_status"
                )
                return RestErrors.forbidden_403(
                    message="Account is inactive. Please contact support.",
                    data=None,
                    errors=[error_detail]
                )
            
            # Check if user is banned
            if user.is_banned:
                log.warning(f"Banned account login attempt: {email}")
                error_detail = ErrorDetail(
                    code="ACCOUNT_BANNED",
                    message="Account is banned. Please contact support.",
                    field="account_status"
                )
                return RestErrors.forbidden_403(
                    message="Account is banned. Please contact support.",
                    data=None,
                    errors=[error_detail]
                )
            
            # Check if user is suspended
            if user.is_suspended:
                log.warning(f"Suspended account login attempt: {email}")
                error_detail = ErrorDetail(
                    code="ACCOUNT_SUSPENDED",
                    message="Account is suspended. Please contact support.",
                    field="account_status"
                )
                return RestErrors.forbidden_403(
                    message="Account is suspended. Please contact support.",
                    data=None,
                    errors=[error_detail]
                )
            
            # Check if user is already logged in (optional business rule)
            if user.is_logged_in:
                log.info(f"User already logged in: {email}")
                # You can choose to allow multiple sessions or reject
                # For now, we'll allow it but log the event
            
            # Check if user's organization is set
            if not user.organization or not user.organization.org_id:
                log.warning(f"User has no organization assigned: {email}")
                error_detail = ErrorDetail(
                    code="NO_ORGANIZATION",
                    message="User must be assigned to an organization. Please contact support.",
                    field="organization"
                )
                return RestErrors.forbidden_403(
                    message="User must be assigned to an organization. Please contact support.",
                    data=None,
                    errors=[error_detail]
                )
            
            # Check if user's email is verified
            if not user.security or not user.security.is_email_verified:
                log.warning(f"Email not verified for user: {email}")
                error_detail = ErrorDetail(
                    code="EMAIL_NOT_VERIFIED",
                    message="Email address must be verified before login. Please check your email for verification link.",
                    field="email_verification"
                )
                return RestErrors.forbidden_403(
                    message="Email address must be verified before login. Please check your email for verification link.",
                    data=None,
                    errors=[error_detail]
                )
            
            # Verify password
            if not user.security or not user.security.password_hash:
                log.error(f"No password hash found for user: {email}")
                error_detail = ErrorDetail(
                    code="ACCOUNT_CONFIG_ERROR",
                    message="Account configuration error",
                    field="password_hash"
                )
                return RestErrors.internal_server_error_500(
                    message="Account configuration error",
                    data=None,
                    errors=[error_detail]
                )
            
            # Use Commons.verify_password to check password
            try:
                password_valid = Commons.verify_password(password, user.security.password_hash)
            except Exception as e:
                log.error(f"Password verification error: {str(e)}")
                error_detail = ErrorDetail(
                    code="PASSWORD_VERIFICATION_ERROR",
                    message="Password verification failed",
                    field="password"
                )
                return RestErrors.internal_server_error_500(
                    message="Password verification failed",
                    data=None,
                    errors=[error_detail]
                )
            
            if not password_valid:
                log.warning(f"Invalid password attempt for: {email}")
                error_detail = ErrorDetail(
                    code="INVALID_CREDENTIALS",
                    message="Invalid email or password",
                    field="email,password"
                )
                return RestErrors.unauthorized_401(
                    message="Invalid email or password",
                    data=None,
                    errors=[error_detail]
                )
            
            log.info(f"Password verified successfully for: {email}")
            
            # Generate JWT tokens
            try:
                user_roles = user.roles if user.roles else ["user"]
                
                # Extract org_id and business_units information
                org_id = None
                business_units = None
                
                if user.organization and user.organization.org_id:
                    org_id = user.organization.org_id
                
                # Extract business unit IDs from the list of BusinessUnit objects
                if user.business_units and len(user.business_units) > 0:
                    business_units = [bu.bu_id for bu in user.business_units]
                
                access_token = self.jwt_service.create_access_token(
                    user.user_id, 
                    user_roles, 
                    org_id=org_id, 
                    business_units=business_units
                )
                refresh_token = self.jwt_service.create_refresh_token(
                    user.user_id, 
                    org_id=org_id, 
                    business_units=business_units
                )
            except Exception as e:
                log.error(f"JWT token generation error: {str(e)}")
                error_detail = ErrorDetail(
                    code="TOKEN_GENERATION_ERROR",
                    message="Token generation failed",
                    field="jwt_tokens"
                )
                return RestErrors.internal_server_error_500(
                    message="Token generation failed",
                    data=None,
                    errors=[error_detail]
                )
            
            # Update login parameters using the dedicated function
            update_success, update_error = self.update_login_parametrs(email)
            if not update_success:
                # Log the error but continue with login (optional: you can choose to fail here)
                log.warning(f"Login parameter update failed for {email}: {update_error.message}")
                # Optionally return the error if timestamp update is critical:
                # return RestErrors.internal_server_error_500(
                #     message=update_error.message,
                #     data=None,
                #     errors=[update_error]
                # )
            
            # Create successful response
            try:
                auth_response = AuthResponse(
                    access_token=access_token,
                    refresh_token=refresh_token,
                    token_type="Bearer",
                    expires_in=3600,  # 1 hour
                )
            except Exception as e:
                log.error(f"Response creation error: {str(e)}")
                error_detail = ErrorDetail(
                    code="RESPONSE_GENERATION_ERROR",
                    message="Response generation failed",
                    field="response"
                )
                return RestErrors.internal_server_error_500(
                    message="Response generation failed",
                    data=None,
                    errors=[error_detail]
                )
            
            log.info(f"Login successful for: {email}")
            
            return RestErrors.success_200(
                message="Login successful",
                data=auth_response.model_dump()
            )
            
        except Exception as e:
            log.error(f"Unexpected error during login: {str(e)}")
            error_detail = ErrorDetail(
                code="UNEXPECTED_ERROR",
                message="An unexpected error occurred during login",
                field="system"
            )
            return RestErrors.internal_server_error_500(
                message="An unexpected error occurred during login",
                data=None,
                errors=[error_detail]
            )

    def refresh_token(self):
        response = RestErrors.created_201(
            message="Resource Created Successfully",
            data={
                "token": "123456",
            }
        )
        return response

    def register(self, user: User): 
        """
        Register a new user with comprehensive validation and error handling.
        
        Args:
            user (User): User registration data 
            
        Returns:
            dict: Response with success/error status and user data
        """
        try:
            # Extract required fields from User model structure
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
                    response_user_data["_id"] = str(response_user_data["_id"])
                
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

    def update_logout_parameters(self, user_id: str):
        """
        Update user logout parameters including timestamps and login status.
        
        Args:
            user_id (str): User's unique identifier
            
        Returns:
            tuple: (success: bool, error_detail: ErrorDetail or None)
        """
        try:
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
                return False, error_detail
            
            # Prepare update data for database
            update_data = {
                "$set": {
                    "metadata.last_activity": current_timestamp,
                    "updated_at": current_timestamp,
                    "is_logged_in": False
                }
            }
            
            # Update user in database using the correct method
            try:
                update_result = self.mongo_client.update_data(
                    "users",
                    {"user_id": user_id},
                    update_data
                )
                
                if not update_result:
                    log.warning(f"Failed to update user logout parameters: {user_id}")
                    error_detail = ErrorDetail(
                        code="DATABASE_UPDATE_FAILED",
                        message="Failed to update user logout parameters",
                        field="database"
                    )
                    return False, error_detail
                else:
                    log.info(f"User logout parameters updated successfully: {user_id}")
                    return True, None
                    
            except Exception as e:
                log.error(f"Error updating user logout data: {str(e)}")
                error_detail = ErrorDetail(
                    code="DATABASE_UPDATE_ERROR",
                    message="Error updating user logout data",
                    field="database"
                )
                return False, error_detail
                
        except Exception as e:
            log.error(f"Unexpected error in update_logout_parameters: {str(e)}")
            error_detail = ErrorDetail(
                code="UNEXPECTED_UPDATE_ERROR",
                message="Unexpected error during logout parameter update",
                field="system"
            )
            return False, error_detail

    def logout(self, access_payload):
        """
        Handle user logout by updating database and invalidating session.
        
        Args:
            access_payload (dict): JWT payload containing user information
            
        Returns:
            dict: Response with success/error status
        """
        try:
            # Input validation
            if not access_payload or not isinstance(access_payload, dict):
                log.error("Invalid access_payload provided to logout")
                error_detail = ErrorDetail(
                    code="INVALID_PAYLOAD",
                    message="Invalid access payload",
                    field="access_payload"
                )
                return RestErrors.bad_request_400(
                    message="Invalid access payload",
                    data=None,
                    errors=[error_detail]
                )
            
            # Extract user_id from access_payload
            user_id = access_payload.get('user_id')
            if not user_id:
                log.error("No user_id found in access_payload")
                error_detail = ErrorDetail(
                    code="MISSING_USER_ID",
                    message="User ID not found in access payload",
                    field="user_id"
                )
                return RestErrors.bad_request_400(
                    message="User ID not found in access payload",
                    data=None,
                    errors=[error_detail]
                )
            
            log.info(f"Logout attempt for user_id: {user_id}")
            
            # Get user data from database to verify user exists
            try:
                user_data = self.mongo_client.find_one("users", {"user_id": user_id})
            except Exception as e:
                log.error(f"Database error during user lookup: {str(e)}")
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
            
            if not user_data:
                log.warning(f"User not found during logout: {user_id}")
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
            
            # Convert dict to User model for easier access
            try:
                user = User(**user_data)
            except Exception as e:
                log.error(f"Error parsing user data: {str(e)}")
                error_detail = ErrorDetail(
                    code="USER_DATA_FORMAT_ERROR",
                    message="User data format error",
                    field="user_data"
                )
                return RestErrors.internal_server_error_500(
                    message="User data format error",
                    data=None,
                    errors=[error_detail]
                )
            
            log.info(f"User data retrieved for logout: {user_id}")
            
            # Check if user is currently logged in
            if not user.is_logged_in:
                log.info(f"User already logged out: {user_id}")
                # Return success even if already logged out (idempotent operation)
                return RestErrors.no_content_204(
                    message="User already logged out",
                    data={"user_id": user_id, "status": "logged_out"}
                )
            
            # Update logout parameters using the dedicated function
            update_success, update_error = self.update_logout_parameters(user_id)
            if not update_success:
                log.error(f"Logout parameter update failed for {user_id}: {update_error.message}")
                return RestErrors.internal_server_error_500(
                    message=update_error.message,
                    data=None,
                    errors=[update_error]
                )
            
            log.info(f"Logout successful for user_id: {user_id}")
            
            return RestErrors.no_content_204(
                message="Logout successful",
                data={
                    "user_id": user_id,
                    "status": "logged_out",
                    "timestamp": Commons.get_timestamp_in_utc()
                }
            )
            
        except Exception as e:
            log.error(f"Unexpected error during logout: {str(e)}")
            error_detail = ErrorDetail(
                code="UNEXPECTED_ERROR",
                message="An unexpected error occurred during logout",
                field="system"
            )
            return RestErrors.internal_server_error_500(
                message="An unexpected error occurred during logout",
                data=None,
                errors=[error_detail]
            )

    def get_me(self, access_payload: dict):
        """
        Retrieve current user's profile data based on access token payload.
        
        Args:
            access_payload (dict): JWT access token payload containing user_id
            
        Returns:
            dict: Response with user data or error details
        """
        try:
            user_id = access_payload.get("user_id")
            if not user_id:
                log.error("No user_id found in access_payload")
                error_detail = ErrorDetail(
                    code="MISSING_USER_ID",
                    message="User ID not found in access payload",
                    field="user_id"
                )
                return RestErrors.bad_request_400(
                    message="User ID not found in access payload",
                    data=None,
                    errors=[error_detail]
                )
            
            # Input validation
            if not isinstance(user_id, str) or not user_id.strip():
                log.error(f"Invalid user_id format: {user_id}")
                error_detail = ErrorDetail(
                    code="INVALID_USER_ID",
                    message="User ID must be a non-empty string",
                    field="user_id"
                )
                return RestErrors.bad_request_400(
                    message="Invalid user ID format",
                    data=None,
                    errors=[error_detail]
                )
            
            user_id = user_id.strip()
            log.info(f"Retrieving user profile for user_id: {user_id}")
            
            # Get user data from database
            try:
                user_data = self.mongo_client.find_one("users", {"user_id": user_id})
            except Exception as e:
                log.error(f"Database error during user lookup for user_id {user_id}: {str(e)}")
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
            
            if not user_data:
                log.warning(f"User not found for user_id: {user_id}")
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
            
            # Convert dict to User model for validation and easier access
            try:
                user = User(**user_data)
            except Exception as e:
                log.error(f"Error parsing user data for user_id {user_id}: {str(e)}")
                error_detail = ErrorDetail(
                    code="USER_DATA_FORMAT_ERROR",
                    message="User data format error",
                    field="user_data"
                )
                return RestErrors.internal_server_error_500(
                    message="User data format error",
                    data=None,
                    errors=[error_detail]
                )
            
            log.info(f"User profile retrieved successfully for user_id: {user_id}")
            
            # Prepare response data - exclude sensitive information
            response_data = user.model_dump()
            
            # Remove sensitive fields from response
            if 'security' in response_data and response_data['security']:
                if 'password_hash' in response_data['security']:
                    del response_data['security']['password_hash']
                if 'recovery_codes' in response_data['security']:
                    del response_data['security']['recovery_codes']
            
            # Convert ObjectId to string if present
            if '_id' in response_data:
                response_data['_id'] = str(response_data['_id'])
            
            return RestErrors.success_200(
                message="User profile retrieved successfully",
                data=response_data
            )
            
        except Exception as e:
            log.error(f"Unexpected error in get_me for user_id {access_payload.get('user_id', 'unknown')}: {str(e)}")
            error_detail = ErrorDetail(
                code="UNEXPECTED_ERROR",
                message="An unexpected error occurred while retrieving user profile",
                field="system"
            )
            return RestErrors.internal_server_error_500(
                message="An unexpected error occurred while retrieving user profile",
                data=None,
                errors=[error_detail]
            )
            
            
