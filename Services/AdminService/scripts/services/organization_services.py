import uuid
from datetime import datetime
from typing import Optional, List

from scripts.utils.logger import log
from scripts.utils.rest_errors import RestErrors
from scripts.databases.mongodb.client import MongoClient
from scripts.services.jwt import JWTService
from scripts.utils.commons import Commons
from scripts.models.organization import Organization
from scripts.models.business_unit import BusinessUnit
from scripts.models.response import ErrorDetail
from scripts.models.request import UserProfile


class OrganizationService:
    def __init__(self, config):
        self.config = config
        mongo_config = self.config.get_mongodb_config()
        self.mongo_client = MongoClient(mongo_config)
        self.jwt_service = JWTService(config)

    def _validate_logged_user_organization(self, logged_user: UserProfile):
        """
        Validate the logged user's organization.
        
        Args:
            logged_user: The authenticated user making the request
            
        Returns:
            dict: Error response if validation fails, None if successful
        """
        try:
            org_id = logged_user.org_id
            org_data = self.mongo_client.find_one(
                "organizations",
                {"org_id": org_id}
            )
            
            if not org_data or org_data.get('status') != 'active':
                log.warning(f"Operation failed: Invalid or inactive organization {org_id}")
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
            return None
        except Exception as e:
            log.error(f"Organization validation error: {str(e)}")
            error_detail = ErrorDetail(
                code="ORGANIZATION_VALIDATION_ERROR",
                message="Organization validation failed",
                field="system"
            )
            return RestErrors.internal_server_error_500(
                message="Organization validation failed",
                data=None,
                errors=[error_detail]
            )

    def add_business_unit(self, org_id: str, business_units: List[str] = None) -> bool:
        """
        Add business unit IDs to an existing organization's business_units list.
        
        Args:
            org_id (str): The unique identifier for the organization
            business_units (List[str], optional): List of business unit IDs to append. Defaults to empty list.
            
        Returns:
            bool: True if business units were added successfully, False otherwise
        """
        try:
            # Initialize business_units list if None
            if business_units is None:
                business_units = []

            # Validate org_id
            if not org_id or not org_id.strip():
                log.error("Organization ID is required")
                return False

            org_id = org_id.strip()
            log.info(f"Adding business unit IDs to organization: {org_id}")

            # Check if organization exists using the existing mongo_client
            try:
                existing_org = self.mongo_client.find_one("organizations", {"org_id": org_id})
            except Exception as e:
                log.error(f"Database error during organization check: {str(e)}")
                return False

            if not existing_org:
                log.warning(f"Organization not found: {org_id}")
                return False

            # Get current business_units list or initialize empty list
            current_business_units = existing_org.get("business_units", [])
            
            # Validate business_units input
            if not isinstance(business_units, list):
                log.error("Business units must be a list of strings")
                return False

            # Validate that all items in business_units are strings
            for idx, bu_id in enumerate(business_units):
                if not isinstance(bu_id, str):
                    log.error(f"Business unit ID at index {idx} must be a string")
                    return False

            # Remove duplicates and filter out empty strings
            valid_business_units = [bu_id.strip() for bu_id in business_units if bu_id and bu_id.strip()]
            
            # Remove duplicates from the new list
            unique_new_business_units = list(set(valid_business_units))
            
            # Check for duplicates with existing business units
            existing_bu_ids = set(current_business_units)
            new_bu_ids = [bu_id for bu_id in unique_new_business_units if bu_id not in existing_bu_ids]
            
            if not new_bu_ids:
                log.info(f"No new business units to add to organization: {org_id}")
                return True  # Consider this as success since no error occurred

            # Append new business unit IDs to existing list
            updated_business_units = current_business_units + new_bu_ids

            # Update the organization in database using existing mongo_client
            try:
                update_result = self.mongo_client.update_data(
                    "organizations",
                    {"org_id": org_id},
                    {"$set": {
                        "business_units": updated_business_units,
                        "updated_at": datetime.utcnow()
                    }}
                )
                
                # Check if update was successful
                if not update_result:
                    log.error(f"Failed to update organization: {org_id}")
                    return False
                    
            except Exception as e:
                log.error(f"Database error during organization update: {str(e)}")
                return False

            log.info(f"Successfully added {len(new_bu_ids)} business unit IDs to organization: {org_id}")
            return True

        except Exception as e:
            log.error(f"Unexpected error in add_business_unit: {str(e)}")
            return False

    def remove_business_unit(self, org_id: str, business_units: List[str] = None) -> bool:
        """
        Remove business unit IDs from an existing organization's business_units list.
        
        Args:
            org_id (str): The unique identifier for the organization
            business_units (List[str], optional): List of business unit IDs to remove. Defaults to empty list.
            
        Returns:
            bool: True if business units were removed successfully, False otherwise
        """
        try:
            # Initialize business_units list if None
            if business_units is None:
                business_units = []

            # Validate org_id
            if not org_id or not org_id.strip():
                log.error("Organization ID is required")
                return False

            org_id = org_id.strip()
            log.info(f"Removing business unit IDs from organization: {org_id}")

            # Check if organization exists using the existing mongo_client
            try:
                existing_org = self.mongo_client.find_one("organizations", {"org_id": org_id})
            except Exception as e:
                log.error(f"Database error during organization check: {str(e)}")
                return False

            if not existing_org:
                log.warning(f"Organization not found: {org_id}")
                return False

            # Get current business_units list or initialize empty list
            current_business_units = existing_org.get("business_units", [])
            
            # Validate business_units input
            if not isinstance(business_units, list):
                log.error("Business units must be a list of strings")
                return False

            # Validate that all items in business_units are strings
            for idx, bu_id in enumerate(business_units):
                if not isinstance(bu_id, str):
                    log.error(f"Business unit ID at index {idx} must be a string")
                    return False

            # Remove duplicates and filter out empty strings from input
            valid_business_units = [bu_id.strip() for bu_id in business_units if bu_id and bu_id.strip()]
            
            # Remove duplicates from the removal list
            unique_removal_business_units = list(set(valid_business_units))
            
            # Check which business units actually exist in the organization
            existing_bu_ids = set(current_business_units)
            bu_ids_to_remove = [bu_id for bu_id in unique_removal_business_units if bu_id in existing_bu_ids]
            
            if not bu_ids_to_remove:
                log.info(f"No business units to remove from organization: {org_id} (none of the provided IDs exist)")
                return True  # Consider this as success since no error occurred

            # Remove business unit IDs from existing list
            updated_business_units = [bu_id for bu_id in current_business_units if bu_id not in bu_ids_to_remove]

            # Update the organization in database using existing mongo_client
            try:
                update_result = self.mongo_client.update_data(
                    "organizations",
                    {"org_id": org_id},
                    {"$set": {
                        "business_units": updated_business_units,
                        "updated_at": Commons.get_timestamp_in_utc()
                    }}
                )
                
                # Check if update was successful
                if not update_result:
                    log.error(f"Failed to update organization: {org_id}")
                    return False
                    
            except Exception as e:
                log.error(f"Database error during organization update: {str(e)}")
                return False

            log.info(f"Successfully removed {len(bu_ids_to_remove)} business unit IDs from organization: {org_id}")
            return True

        except Exception as e:
            log.error(f"Unexpected error in remove_business_unit: {str(e)}")
            return False

    # Organization CRUD operations

    # Organization CRUD operations
    def create_organization(self, logged_user: UserProfile, organization: Organization):
        """
        Create a new organization with comprehensive validation and error handling.
        
        Args:
            logged_user (UserProfile): The authenticated user making the request
            organization (Organization): Organization data to create
            
        Returns:
            dict: Response with success/error status and organization data
        """
        try:
            # Validate logged user's organization
            # validation_error = self._validate_logged_user_organization(logged_user)
            # if validation_error:
            #     return validation_error

            # Extract and validate required fields
            name = organization.name.strip() if organization.name else None
            org_id = organization.org_id.strip() if organization.org_id else None

            if not name:
                error_detail = ErrorDetail(
                    code="MISSING_ORGANIZATION_NAME",
                    message="Organization name is required",
                    field="name"
                )
                return RestErrors.bad_request_400(
                    message="Organization name is required",
                    data=None,
                    errors=[error_detail]
                )

            log.info(f"Creating organization: {name} by user: {logged_user.user_id}")

            # Generate org_id if not provided
            if not org_id:
                org_id = str(uuid.uuid4())

            # Check if org_id already exists
            try:
                existing_org = self.mongo_client.find_one("organizations", {"org_id": org_id})
            except Exception as e:
                log.error(f"Database error during org_id check: {str(e)}")
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

            if existing_org:
                log.warning(f"Organization ID already exists: {org_id}")
                error_detail = ErrorDetail(
                    code="ORG_ID_ALREADY_EXISTS",
                    message="Organization ID already exists",
                    field="org_id"
                )
                return RestErrors.bad_request_400(
                    message="Organization ID already exists",
                    data=None,
                    errors=[error_detail]
                )

            # Check if organization name already exists
            try:
                existing_name_org = self.mongo_client.find_one("organizations", {"name": name})
            except Exception as e:
                log.error(f"Database error during name check: {str(e)}")
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

            if existing_name_org:
                log.warning(f"Organization name already exists: {name}")
                error_detail = ErrorDetail(
                    code="ORG_NAME_ALREADY_EXISTS",
                    message="Organization name already exists",
                    field="name"
                )
                return RestErrors.bad_request_400(
                    message="Organization name already exists",
                    data=None,
                    errors=[error_detail]
                )

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

            # Create comprehensive organization data with all hierarchical parameters and default values
            try:
                org_data = {
                    # Core identification fields
                    "org_id": org_id,
                    "name": name,
                    "is_active": organization.is_active if organization.is_active is not None else True,
                    "short_name": organization.short_name if organization.short_name else None,
                    "description": organization.description if organization.description else None,
                    
                    # Contact and Location section with defaults
                    "primary_contact": organization.primary_contact if organization.primary_contact else None,
                    
                    # Contact and Location section with defaults
                    "email": str(organization.email) if organization.email else None,
                    "website": str(organization.website) if organization.website else None,
                    
                    # Address section with defaults
                    "address": {
                        "street": organization.address.street if organization.address and hasattr(organization.address, 'street') and organization.address.street else None,
                        "city": organization.address.city if organization.address and hasattr(organization.address, 'city') and organization.address.city else None,
                        "state": organization.address.state if organization.address and hasattr(organization.address, 'state') and organization.address.state else None,
                        "zip_code": organization.address.zip_code if organization.address and hasattr(organization.address, 'zip_code') and organization.address.zip_code else None,
                        "country": organization.address.country if organization.address and hasattr(organization.address, 'country') and organization.address.country else None
                    } if organization.address and not isinstance(organization.address, str) else organization.address if isinstance(organization.address, str) else {
                        "street": None,
                        "city": None,
                        "state": None,
                        "zip_code": None,
                        "country": None
                    },
                    
                    # Hierarchy and Relationships section with defaults
                    "parent_org_id": organization.parent_org_id if organization.parent_org_id else None,
                    "status": organization.status if organization.status else "active",
                    
                    # Business units section with defaults
                    "business_units": organization.business_units if organization.business_units else [],
                    
                    # Membership and Resources section with defaults
                    "members": organization.members if organization.members else [],
                    "projects": organization.projects if organization.projects else [],
                    
                    # Timestamps section with defaults
                    "established_date": organization.established_date if organization.established_date else None,
                    "created_at": organization.created_at if organization.created_at else current_timestamp,
                    "updated_at": current_timestamp,
                    
                    # Metadata section with defaults - always include structure
                    "metadata": organization.metadata if organization.metadata else {},

                }
                
                # Validate the complete organization data with Organization model
                validated_org = Organization(**org_data)
                
            except Exception as e:
                log.error(f"Organization model creation error: {str(e)}")
                error_detail = ErrorDetail(
                    code="ORGANIZATION_MODEL_ERROR",
                    message="Organization data validation failed",
                    field="organization_data"
                )
                return RestErrors.internal_server_error_500(
                    message="Organization data validation failed",
                    data=None,
                    errors=[error_detail]
                )

            # Insert organization into database
            try:
                insert_result = self.mongo_client.insert_data("organizations", org_data, document_id=org_id)
                
                if not insert_result:
                    log.error(f"Failed to insert organization data for: {name}")
                    error_detail = ErrorDetail(
                        code="DATABASE_INSERT_FAILED",
                        message="Failed to create organization",
                        field="database"
                    )
                    return RestErrors.internal_server_error_500(
                        message="Failed to create organization",
                        data=None,
                        errors=[error_detail]
                    )
                
                log.info(f"Organization created successfully: {name}")
                
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

            # Prepare response data
            try:
                response_data = org_data.copy()
                
                # Convert ObjectId to string if present (MongoDB adds _id field)
                if "_id" in response_data:
                    del response_data["_id"]
                
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

            log.info(f"Organization creation completed successfully for: {name}")

            return RestErrors.created_201(
                message="Organization created successfully",
                data=response_data
            )

        except Exception as e:
            log.error(f"Unexpected error during organization creation: {str(e)}")
            error_detail = ErrorDetail(
                code="UNEXPECTED_ERROR",
                message="An unexpected error occurred during organization creation",
                field="system"
            )
            return RestErrors.internal_server_error_500(
                message="An unexpected error occurred during organization creation",
                data=None,
                errors=[error_detail]
            )

    def get_organization(self, logged_user: UserProfile, org_id: str):
        """
        Retrieve an organization by ID with comprehensive validation and error handling.
        
        Args:
            logged_user (UserProfile): The authenticated user making the request
            org_id (str): The unique identifier for the organization
            
        Returns:
            dict: Response with success/error status and organization data
        """
        try:
            # Validate logged user's organization
            validation_error = self._validate_logged_user_organization(logged_user)
            if validation_error:
                return validation_error

            # Input validation
            if not org_id or not org_id.strip():
                error_detail = ErrorDetail(
                    code="MISSING_ORG_ID",
                    message="Organization ID is required",
                    field="org_id"
                )
                return RestErrors.bad_request_400(
                    message="Organization ID is required",
                    data=None,
                    errors=[error_detail]
                )

            org_id = org_id.strip()
            log.info(f"Retrieving organization with ID: {org_id}")

            # Query organization from database
            try:
                org_data = self.mongo_client.find_one("organizations", {"org_id": org_id})
            except Exception as e:
                log.error(f"Database error during organization retrieval: {str(e)}")
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

            # Check if organization exists
            if not org_data:
                log.warning(f"Organization not found with ID: {org_id}")
                error_detail = ErrorDetail(
                    code="ORGANIZATION_NOT_FOUND",
                    message="Organization not found",
                    field="org_id"
                )
                return RestErrors.not_found_404(
                    message="Organization not found",
                    data=None,
                    errors=[error_detail]
                )

            # Validate organization data with Organization model
            try:
                if "_id" in org_data:
                    del org_data["_id"]
                organization = Organization(**org_data)
            except Exception as e:
                log.error(f"Organization model validation error: {str(e)}")
                error_detail = ErrorDetail(
                    code="ORGANIZATION_MODEL_ERROR",
                    message="Organization data validation failed",
                    field="organization_data"
                )
                return RestErrors.internal_server_error_500(
                    message="Organization data validation failed",
                    data=None,
                    errors=[error_detail]
                )

            log.info(f"Organization retrieved successfully: {org_id}")

            return RestErrors.success_200(
                message="Organization retrieved successfully",
                data=org_data
            )

        except Exception as e:
            log.error(f"Unexpected error during organization retrieval: {str(e)}")
            error_detail = ErrorDetail(
                code="UNEXPECTED_ERROR",
                message="An unexpected error occurred during organization retrieval",
                field="system"
            )
            return RestErrors.internal_server_error_500(
                message="An unexpected error occurred during organization retrieval",
                data=None,
                errors=[error_detail]
            )

    def update_organization(self, logged_user: UserProfile, organization: Organization, org_id: str):
        """
        Update an existing organization with comprehensive validation and error handling.
        Only updates fields that are provided and valid in the request.
        
        Args:
            logged_user (UserProfile): The authenticated user making the request
            organization (Organization): Organization data with fields to update
            org_id (str): The unique identifier for the organization to update
            
        Returns:
            dict: Response with success/error status and updated organization data
        """
        try:
            # Validate logged user's organization
            validation_error = self._validate_logged_user_organization(logged_user)
            if validation_error:
                return validation_error

            # Validate org_id
            if not org_id or not org_id.strip():
                error_detail = ErrorDetail(
                    code="MISSING_ORG_ID",
                    message="Organization ID is required",
                    field="org_id"
                )
                return RestErrors.bad_request_400(
                    message="Organization ID is required",
                    data=None,
                    errors=[error_detail]
                )

            org_id = org_id.strip()
            log.info(f"Updating organization with ID: {org_id}")

            # Get existing organization from database
            try:
                existing_org_data = self.mongo_client.find_one("organizations", {"org_id": org_id})
            except Exception as e:
                log.error(f"Database error during organization retrieval: {str(e)}")
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

            # Check if organization exists
            if not existing_org_data:
                log.warning(f"Organization not found with ID: {org_id}")
                error_detail = ErrorDetail(
                    code="ORGANIZATION_NOT_FOUND",
                    message="Organization not found",
                    field="org_id"
                )
                return RestErrors.not_found_404(
                    message="Organization not found",
                    data=None,
                    errors=[error_detail]
                )

            # Prepare update data by checking each field in the hierarchy
            update_data = {}
            invalid_fields = []

            # Define valid top-level fields from Organization model
            valid_top_level_fields = {
                'org_id', 'name', 'is_active', 'short_name', 'description', 
                'primary_contact', 'email', 'website', 'address', 'parent_org_id', 
                'status', 'business_units', 'members', 'projects', 'established_date', 
                'created_at', 'updated_at', 'metadata'
            }

            # Check for invalid top-level fields in the request
            org_dict = organization.dict(exclude_unset=True)
            for field_name in org_dict.keys():
                if field_name not in valid_top_level_fields:
                    invalid_fields.append(field_name)

            # If there are invalid top-level fields, return error immediately
            if invalid_fields:
                log.warning(f"Invalid top-level fields provided for organization update: {invalid_fields}")
                error_details = []
                for field in invalid_fields:
                    error_details.append(ErrorDetail(
                        code="INVALID_FIELD",
                        message=f"Field '{field}' is not a valid top-level field",
                        field=field
                    ))
                return RestErrors.bad_request_400(
                    message=f"Invalid top-level fields provided: {', '.join(invalid_fields)}",
                    data=None,
                    errors=error_details
                )

            # Top-level fields validation
            if organization.name is not None:
                if 'name' not in existing_org_data:
                    invalid_fields.append('name')
                else:
                    name = organization.name.strip()
                    # Check if name is already taken by another organization
                    try:
                        existing_name_org = self.mongo_client.find_one("organizations", {"name": name, "org_id": {"$ne": org_id}})
                        if existing_name_org:
                            error_detail = ErrorDetail(
                                code="ORG_NAME_ALREADY_EXISTS",
                                message="Organization name is already taken by another organization",
                                field="name"
                            )
                            return RestErrors.bad_request_400(
                                message="Organization name is already taken by another organization",
                                data=None,
                                errors=[error_detail]
                            )
                        update_data["name"] = name
                    except Exception as e:
                        log.error(f"Database error during name check: {str(e)}")
                        error_detail = ErrorDetail(
                            code="DATABASE_ERROR",
                            message="Database connection error during name validation",
                            field="system"
                        )
                        return RestErrors.internal_server_error_500(
                            message="Database connection error during name validation",
                            data=None,
                            errors=[error_detail]
                        )

            if organization.is_active is not None:
                if 'is_active' not in existing_org_data:
                    invalid_fields.append('is_active')
                else:
                    update_data["is_active"] = organization.is_active

            if organization.short_name is not None:
                if 'short_name' not in existing_org_data:
                    invalid_fields.append('short_name')
                else:
                    update_data["short_name"] = organization.short_name

            if organization.description is not None:
                if 'description' not in existing_org_data:
                    invalid_fields.append('description')
                else:
                    update_data["description"] = organization.description

            if organization.email is not None:
                if 'email' not in existing_org_data:
                    invalid_fields.append('email')
                else:
                    update_data["email"] = str(organization.email)

            if organization.website is not None:
                if 'website' not in existing_org_data:
                    invalid_fields.append('website')
                else:
                    update_data["website"] = str(organization.website)

            if organization.parent_org_id is not None:
                if 'parent_org_id' not in existing_org_data:
                    invalid_fields.append('parent_org_id')
                else:
                    update_data["parent_org_id"] = organization.parent_org_id

            if organization.status is not None:
                if 'status' not in existing_org_data:
                    invalid_fields.append('status')
                else:
                    update_data["status"] = organization.status

            if organization.members is not None:
                if 'members' not in existing_org_data:
                    invalid_fields.append('members')
                else:
                    update_data["members"] = organization.members

            if organization.projects is not None:
                if 'projects' not in existing_org_data:
                    invalid_fields.append('projects')
                else:
                    update_data["projects"] = organization.projects

            if organization.established_date is not None:
                if 'established_date' not in existing_org_data:
                    invalid_fields.append('established_date')
                else:
                    update_data["established_date"] = organization.established_date

            if organization.metadata is not None:
                if 'metadata' not in existing_org_data:
                    invalid_fields.append('metadata')
                else:
                    update_data["metadata"] = organization.metadata

            # Primary contact nested object validation
            if organization.primary_contact is not None:
                if 'primary_contact' not in existing_org_data:
                    invalid_fields.append('primary_contact')
                else:
                    contact_updates = {}
                    existing_contact = existing_org_data.get('primary_contact', {})

                    if organization.primary_contact.contact_id is not None:
                        if 'contact_id' not in existing_contact:
                            invalid_fields.append('primary_contact.contact_id')
                        else:
                            contact_updates["primary_contact.contact_id"] = organization.primary_contact.contact_id

                    if organization.primary_contact.name is not None:
                        if 'name' not in existing_contact:
                            invalid_fields.append('primary_contact.name')
                        else:
                            contact_updates["primary_contact.name"] = organization.primary_contact.name

                    if organization.primary_contact.username is not None:
                        if 'username' not in existing_contact:
                            invalid_fields.append('primary_contact.username')
                        else:
                            contact_updates["primary_contact.username"] = organization.primary_contact.username

                    if organization.primary_contact.email is not None:
                        if 'email' not in existing_contact:
                            invalid_fields.append('primary_contact.email')
                        else:
                            contact_updates["primary_contact.email"] = str(organization.primary_contact.email)

                    if organization.primary_contact.phone_number is not None:
                        if 'phone_number' not in existing_contact:
                            invalid_fields.append('primary_contact.phone_number')
                        else:
                            contact_updates["primary_contact.phone_number"] = organization.primary_contact.phone_number

                    if organization.primary_contact.role is not None:
                        if 'role' not in existing_contact:
                            invalid_fields.append('primary_contact.role')
                        else:
                            contact_updates["primary_contact.role"] = organization.primary_contact.role

                    update_data.update(contact_updates)

            # Address nested object validation
            if organization.address is not None:
                if 'address' not in existing_org_data:
                    invalid_fields.append('address')
                else:
                    # Handle both string and structured address
                    if isinstance(organization.address, str):
                        update_data["address"] = organization.address
                    else:
                        address_updates = {}
                        existing_address = existing_org_data.get('address', {})

                        if organization.address.street is not None:
                            if 'street' not in existing_address:
                                invalid_fields.append('address.street')
                            else:
                                address_updates["address.street"] = organization.address.street

                        if organization.address.city is not None:
                            if 'city' not in existing_address:
                                invalid_fields.append('address.city')
                            else:
                                address_updates["address.city"] = organization.address.city

                        if organization.address.state is not None:
                            if 'state' not in existing_address:
                                invalid_fields.append('address.state')
                            else:
                                address_updates["address.state"] = organization.address.state

                        if organization.address.zip_code is not None:
                            if 'zip_code' not in existing_address:
                                invalid_fields.append('address.zip_code')
                            else:
                                address_updates["address.zip_code"] = organization.address.zip_code

                        if organization.address.country is not None:
                            if 'country' not in existing_address:
                                invalid_fields.append('address.country')
                            else:
                                address_updates["address.country"] = organization.address.country

                        update_data.update(address_updates)

            # Business units array validation
            if organization.business_units is not None:
                if 'business_units' not in existing_org_data:
                    invalid_fields.append('business_units')
                else:
                    business_units_data = []
                    for bu in organization.business_units:
                        bu_data = {}
                        if bu.bu_id is not None:
                            bu_data["bu_id"] = bu.bu_id
                        if bu.name is not None:
                            bu_data["name"] = bu.name
                        if bu.description is not None:
                            bu_data["description"] = bu.description
                        if bu.parent_org is not None:
                            bu_data["parent_org"] = {
                                "org_id": bu.parent_org.org_id if bu.parent_org.org_id else None,
                                "org_name": bu.parent_org.org_name if bu.parent_org.org_name else None
                            }
                        if bu.parent_bu_id is not None:
                            bu_data["parent_bu_id"] = bu.parent_bu_id
                        if bu.head is not None:
                            bu_data["head"] = {
                                "user_id": bu.head.user_id if bu.head.user_id else None,
                                "username": bu.head.username if bu.head.username else None
                            }
                        if bu.members is not None:
                            bu_data["members"] = bu.members
                        if bu.projects is not None:
                            bu_data["projects"] = bu.projects
                        if bu.status is not None:
                            bu_data["status"] = bu.status
                        if bu.created_at is not None:
                            bu_data["created_at"] = bu.created_at
                        if bu.updated_at is not None:
                            bu_data["updated_at"] = bu.updated_at
                        if bu.metadata is not None:
                            bu_data["metadata"] = bu.metadata
                        business_units_data.append(bu_data)
                    update_data["business_units"] = business_units_data

            # Check if there are invalid fields
            if invalid_fields:
                log.warning(f"Invalid fields provided for organization update: {invalid_fields}")
                error_details = []
                for field in invalid_fields:
                    error_details.append(ErrorDetail(
                        code="INVALID_FIELD",
                        message=f"Field '{field}' does not exist in organization data structure",
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
                log.warning(f"No valid fields provided for update for organization: {org_id}")
                error_detail = ErrorDetail(
                    code="NO_FIELDS_TO_UPDATE",
                    message="No valid fields provided for update",
                    field="organization_data"
                )
                return RestErrors.bad_request_400(
                    message="No valid fields provided for update",
                    data=None,
                    errors=[error_detail]
                )

            # Update organization in database
            try:
                result = self.mongo_client.update_data(
                    collection_name="organizations",
                    filter_dict={"org_id": org_id},
                    update_dict={"$set": update_data},
                    upsert=False,
                    update_many=False
                )

                if not result:
                    log.warning(f"No changes made to organization: {org_id}")
                    error_detail = ErrorDetail(
                        code="NO_CHANGES_MADE",
                        message="No changes were made to the organization",
                        field="organization_data"
                    )
                    return RestErrors.bad_request_400(
                        message="No changes were made to the organization",
                        data=None,
                        errors=[error_detail]
                    )

            except Exception as e:
                log.error(f"Database error during organization update: {str(e)}")
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

            # Retrieve updated organization data
            try:
                updated_org_data = self.mongo_client.find_one("organizations", {"org_id": org_id})
            except Exception as e:
                log.error(f"Database error during updated organization retrieval: {str(e)}")
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

            # Prepare response data
            try:
                response_org_data = updated_org_data.copy()

                # Convert ObjectId to string if present
                if "_id" in response_org_data:
                    del response_org_data["_id"]

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

            log.info(f"Organization updated successfully: {org_id}")

            return RestErrors.success_200(
                message="Organization updated successfully",
                data=response_org_data
            )

        except Exception as e:
            log.error(f"Unexpected error during organization update: {str(e)}")
            error_detail = ErrorDetail(
                code="UNEXPECTED_ERROR",
                message="An unexpected error occurred during organization update",
                field="system"
            )
            return RestErrors.internal_server_error_500(
                message="An unexpected error occurred during organization update",
                data=None,
                errors=[error_detail]
            )

    def delete_organization(self, logged_user: UserProfile, org_id: str):
        """
        Delete an organization with comprehensive validation and error handling.
        
        Args:
            logged_user (UserProfile): The authenticated user making the request
            org_id (str): The unique identifier for the organization
            
        Returns:
            dict: Response with success/error status
        """
        try:
            # Validate logged user's organization
            validation_error = self._validate_logged_user_organization(logged_user)
            if validation_error:
                return validation_error

            # Input validation
            if not org_id or not org_id.strip():
                error_detail = ErrorDetail(
                    code="MISSING_ORG_ID",
                    message="Organization ID is required",
                    field="org_id"
                )
                return RestErrors.bad_request_400(
                    message="Organization ID is required",
                    data=None,
                    errors=[error_detail]
                )

            org_id = org_id.strip()
            log.info(f"Deleting organization: {org_id} by user: {logged_user.user_id}")

            # Check if organization exists
            try:
                existing_org = self.mongo_client.find_one("organizations", {"org_id": org_id})
            except Exception as e:
                log.error(f"Database error during organization check: {str(e)}")
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

            if not existing_org:
                log.warning(f"Organization not found for deletion: {org_id}")
                error_detail = ErrorDetail(
                    code="ORGANIZATION_NOT_FOUND",
                    message="Organization not found",
                    field="org_id"
                )
                return RestErrors.not_found_404(
                    message="Organization not found",
                    data=None,
                    errors=[error_detail]
                )

            # Check for dependent business units
            try:
                dependent_bus = self.mongo_client.find_many(
                    "business_units", 
                    {"parent_org": org_id}
                )
                if dependent_bus and len(list(dependent_bus)) > 0:
                    log.warning(f"Cannot delete organization with dependent business units: {org_id}")
                    error_detail = ErrorDetail(
                        code="ORGANIZATION_HAS_DEPENDENCIES",
                        message="Cannot delete organization with existing business units",
                        field="org_id"
                    )
                    return RestErrors.bad_request_400(
                        message="Cannot delete organization with existing business units",
                        data=None,
                        errors=[error_detail]
                    )
            except Exception as e:
                log.error(f"Database error during dependency check: {str(e)}")
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

            # Delete organization from database
            try:
                result = self.mongo_client.delete_data("organizations", {"org_id": org_id})
                if not result:
                    raise Exception("Failed to delete organization")
            except Exception as e:
                log.error(f"Database error during organization deletion: {str(e)}")
                error_detail = ErrorDetail(
                    code="DATABASE_ERROR",
                    message="Failed to delete organization",
                    field="system"
                )
                return RestErrors.internal_server_error_500(
                    message="Failed to delete organization",
                    data=None,
                    errors=[error_detail]
                )

            log.info(f"Organization deleted successfully: {org_id}")

            return RestErrors.no_content_204(
                message="Organization deleted successfully",
                data = {
                    "org_id": org_id
                }
            )

        except Exception as e:
            log.error(f"Unexpected error during organization deletion: {str(e)}")
            error_detail = ErrorDetail(
                code="UNEXPECTED_ERROR",
                message="An unexpected error occurred during organization deletion",
                field="system"
            )
            return RestErrors.internal_server_error_500(
                message="An unexpected error occurred during organization deletion",
                data=None,
                errors=[error_detail]
            )

    def get_organizations(self, logged_user: UserProfile, limit: int = 100, skip: int = 0):
        """
        Retrieve all organizations with comprehensive validation, pagination, and error handling.
        
        Args:
            logged_user (UserProfile): The authenticated user making the request
            limit (int): Maximum number of organizations to return (default: 100, max: 1000)
            skip (int): Number of organizations to skip for pagination (default: 0)
            
        Returns:
            dict: Response with success/error status and list of organization data
        """
        try:
            # Validate logged user's organization
            # validation_error = self._validate_logged_user_organization(logged_user)
            # if validation_error:
            #     return validation_error

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

            log.info(f"Retrieving organizations, limit: {limit}, skip: {skip}")

            # Query organizations from database
            try:
                query_filter = {}  # Get all organizations
                
                # Get total count for pagination metadata
                total_count = self.mongo_client.count_documents("organizations", query_filter)
                
                # Query organizations with pagination
                orgs_data = self.mongo_client.find_many(
                    "organizations", 
                    query_filter, 
                    limit=limit, 
                    skip=skip,
                    sort=[("created_at", -1)]  # Sort by newest first
                )
                
            except Exception as e:
                log.error(f"Database error during organizations retrieval: {str(e)}")
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

            # Process and validate organization data
            try:
                processed_orgs = []
                
                for org_data in orgs_data:
                    # Validate organization data with Organization model
                    try:
                        if "_id" in org_data:
                            del org_data["_id"]
                        organization = Organization(**org_data)
                    except Exception as e:
                        log.warning(f"Organization model validation failed for org {org_data.get('org_id', 'unknown')}: {str(e)}")
                        continue  # Skip invalid organizations instead of failing the entire request
                    
                    processed_orgs.append(org_data)
                
            except Exception as e:
                log.error(f"Organization data processing error: {str(e)}")
                error_detail = ErrorDetail(
                    code="DATA_PROCESSING_ERROR",
                    message="Organization data processing failed",
                    field="organization_data"
                )
                return RestErrors.internal_server_error_500(
                    message="Organization data processing failed",
                    data=None,
                    errors=[error_detail]
                )

            # Prepare pagination metadata
            pagination_metadata = {
                "total_count": total_count,
                "returned_count": len(processed_orgs),
                "limit": limit,
                "skip": skip,
                "has_more": (skip + len(processed_orgs)) < total_count
            }

            # Prepare final response data
            response_data = {
                "organizations": processed_orgs,
                "pagination": pagination_metadata
            }

            log.info(f"Organizations retrieved successfully: {len(processed_orgs)} organizations")

            return RestErrors.success_200(
                message=f"Organizations retrieved successfully. Found {len(processed_orgs)} organizations.",
                data=response_data
            )

        except Exception as e:
            log.error(f"Unexpected error during organizations retrieval: {str(e)}")
            error_detail = ErrorDetail(
                code="UNEXPECTED_ERROR",
                message="An unexpected error occurred during organizations retrieval",
                field="system"
            )
            return RestErrors.internal_server_error_500(
                message="An unexpected error occurred during organizations retrieval",
                data=None,
                errors=[error_detail]
            )

    # Business Unit CRUD operations
    def create_business_unit(self, logged_user: UserProfile, business_unit: BusinessUnit, org_id: str):
        """
        Create a new business unit within an organization with comprehensive validation and error handling.
        
        Args:
            logged_user (UserProfile): The authenticated user making the request
            business_unit (BusinessUnit): Business unit data to create
            org_id (str): The organization ID where the business unit will be created
            
        Returns:
            dict: Response with success/error status and business unit data
        """
        try:
            # Validate logged user's organization
            validation_error = self._validate_logged_user_organization(logged_user)
            if validation_error:
                return validation_error

            # Input validation
            if not org_id or not org_id.strip():
                error_detail = ErrorDetail(
                    code="MISSING_ORG_ID",
                    message="Organization ID is required",
                    field="org_id"
                )
                return RestErrors.bad_request_400(
                    message="Organization ID is required",
                    data=None,
                    errors=[error_detail]
                )

            org_id = org_id.strip()

            # Extract and validate required fields
            name = business_unit.name.strip() if business_unit.name else None
            bu_id = business_unit.bu_id.strip() if business_unit.bu_id else None

            if not name:
                error_detail = ErrorDetail(
                    code="MISSING_BUSINESS_UNIT_NAME",
                    message="Business unit name is required",
                    field="name"
                )
                return RestErrors.bad_request_400(
                    message="Business unit name is required",
                    data=None,
                    errors=[error_detail]
                )

            log.info(f"Creating business unit: {name} in organization: {org_id} by user: {logged_user.user_id}")

            # Generate bu_id if not provided
            if not bu_id:
                bu_id = str(uuid.uuid4())

            # Check if parent organization exists
            try:
                parent_org = self.mongo_client.find_one("organizations", {"org_id": org_id})
            except Exception as e:
                log.error(f"Database error during parent organization check: {str(e)}")
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

            if not parent_org:
                log.warning(f"Parent organization not found: {org_id}")
                error_detail = ErrorDetail(
                    code="PARENT_ORGANIZATION_NOT_FOUND",
                    message="Parent organization not found",
                    field="org_id"
                )
                return RestErrors.not_found_404(
                    message="Parent organization not found",
                    data=None,
                    errors=[error_detail]
                )

            # Check if bu_id already exists
            try:
                existing_bu = self.mongo_client.find_one("business_units", {"bu_id": bu_id})
            except Exception as e:
                log.error(f"Database error during bu_id check: {str(e)}")
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

            if existing_bu:
                log.warning(f"Business unit ID already exists: {bu_id}")
                error_detail = ErrorDetail(
                    code="BU_ID_ALREADY_EXISTS",
                    message="Business unit ID already exists",
                    field="bu_id"
                )
                return RestErrors.bad_request_400(
                    message="Business unit ID already exists",
                    data=None,
                    errors=[error_detail]
                )

            # Check if business unit name already exists within the organization
            try:
                existing_name_bu = self.mongo_client.find_one(
                    "business_units", 
                    {"name": name, "parent_org": org_id}
                )
            except Exception as e:
                log.error(f"Database error during name check: {str(e)}")
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

            if existing_name_bu:
                log.warning(f"Business unit name already exists in organization: {name}")
                error_detail = ErrorDetail(
                    code="BU_NAME_ALREADY_EXISTS",
                    message="Business unit name already exists in this organization",
                    field="name"
                )
                return RestErrors.bad_request_400(
                    message="Business unit name already exists in this organization",
                    data=None,
                    errors=[error_detail]
                )

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

            # Create comprehensive business unit data with all hierarchical parameters and default values
            try:
                bu_data = {
                    # Core identification fields
                    "bu_id": bu_id,
                    "name": name,
                    "description": business_unit.description if business_unit.description else None,
                    
                    # Hierarchy and Relationships section with defaults
                    "parent_org": business_unit.parent_org if business_unit.parent_org else None,
                    "parent_bu_id": business_unit.parent_bu_id if business_unit.parent_bu_id else None,
                    
                    # Business unit head section with defaults
                    "head": business_unit.head if business_unit.head else None,
                    
                    # Resources section with defaults
                    "members": business_unit.members if business_unit.members else [],
                    "projects": business_unit.projects if business_unit.projects else [],
                    
                    # Status and Lifecycle section with defaults
                    "status": business_unit.status if business_unit.status else "active",
                    
                    # Timestamps section with defaults
                    "created_at": business_unit.created_at if business_unit.created_at else current_timestamp,
                    "updated_at": current_timestamp,
                    
                    # Metadata section with defaults - always include structure
                    "metadata": business_unit.metadata if business_unit.metadata else {}
                }
                
                # Validate the complete business unit data with BusinessUnit model
                validated_bu = BusinessUnit(**bu_data)
                
            except Exception as e:
                log.error(f"Business unit model creation error: {str(e)}")
                error_detail = ErrorDetail(
                    code="BUSINESS_UNIT_MODEL_ERROR",
                    message="Business unit data validation failed",
                    field="business_unit_data"
                )
                return RestErrors.internal_server_error_500(
                    message="Business unit data validation failed",
                    data=None,
                    errors=[error_detail]
                )

            # Insert business unit into database using insert_data method
            try:
                insert_result = self.mongo_client.insert_data("business_units", bu_data, document_id=bu_id)
                
                if not insert_result:
                    log.error(f"Failed to insert business unit data for: {name}")
                    error_detail = ErrorDetail(
                        code="DATABASE_INSERT_FAILED",
                        message="Failed to create business unit",
                        field="database"
                    )
                    return RestErrors.internal_server_error_500(
                        message="Failed to create business unit",
                        data=None,
                        errors=[error_detail]
                    )
                
                log.info(f"Business unit created successfully: {name}")
                
                # Add the business unit ID to the organization's business_units list
                try:
                    org_update_success = self.add_business_unit(org_id, [bu_id])
                    if not org_update_success:
                        log.warning(f"Business unit created but failed to update organization {org_id} with business unit {bu_id}")
                        # Note: We don't return error here as the business unit was created successfully
                        # The organization update failure is logged but doesn't fail the entire operation
                    else:
                        log.info(f"Successfully added business unit {bu_id} to organization {org_id}")
                except Exception as e:
                    log.error(f"Error updating organization with new business unit: {str(e)}")
                    # Note: We don't return error here as the business unit was created successfully
                
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

            # Prepare response data
            try:
                response_data = bu_data.copy()
                
                # Convert ObjectId to string if present (MongoDB adds _id field)
                if "_id" in response_data:
                    del response_data["_id"]
                
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

            log.info(f"Business unit creation completed successfully for: {name}")

            return RestErrors.created_201(
                message="Business unit created successfully",
                data=response_data
            )

        except Exception as e:
            log.error(f"Unexpected error during business unit creation: {str(e)}")
            error_detail = ErrorDetail(
                code="UNEXPECTED_ERROR",
                message="An unexpected error occurred during business unit creation",
                field="system"
            )
            return RestErrors.internal_server_error_500(
                message="An unexpected error occurred during business unit creation",
                data=None,
                errors=[error_detail]
            )

    def get_business_unit(self, logged_user: UserProfile, bu_id: str, org_id: str):
        """
        Retrieve a business unit by ID within an organization with comprehensive validation and error handling.
        
        Args:
            logged_user (UserProfile): The authenticated user making the request
            bu_id (str): The unique identifier for the business unit
            org_id (str): The organization ID where the business unit belongs
            
        Returns:
            dict: Response with success/error status and business unit data
        """
        try:
            # Validate logged user's organization
            validation_error = self._validate_logged_user_organization(logged_user)
            if validation_error:
                return validation_error

            # Input validation
            if not bu_id or not bu_id.strip():
                error_detail = ErrorDetail(
                    code="MISSING_BU_ID",
                    message="Business unit ID is required",
                    field="bu_id"
                )
                return RestErrors.bad_request_400(
                    message="Business unit ID is required",
                    data=None,
                    errors=[error_detail]
                )

            if not org_id or not org_id.strip():
                error_detail = ErrorDetail(
                    code="MISSING_ORG_ID",
                    message="Organization ID is required",
                    field="org_id"
                )
                return RestErrors.bad_request_400(
                    message="Organization ID is required",
                    data=None,
                    errors=[error_detail]
                )

            bu_id = bu_id.strip()
            org_id = org_id.strip()
            log.info(f"Retrieving business unit with ID: {bu_id} in organization: {org_id}")

            # Query business unit from database
            try:
                bu_data = self.mongo_client.find_one(
                    "business_units", 
                    {"bu_id": bu_id, "parent_org": org_id}
                )
            except Exception as e:
                log.error(f"Database error during business unit retrieval: {str(e)}")
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

            # Check if business unit exists
            if not bu_data:
                log.warning(f"Business unit not found with ID: {bu_id} in organization: {org_id}")
                error_detail = ErrorDetail(
                    code="BUSINESS_UNIT_NOT_FOUND",
                    message="Business unit not found",
                    field="bu_id"
                )
                return RestErrors.not_found_404(
                    message="Business unit not found",
                    data=None,
                    errors=[error_detail]
                )

            # Validate business unit data with BusinessUnit model
            try:
                if "_id" in bu_data:
                    del bu_data["_id"]
                business_unit = BusinessUnit(**bu_data)
            except Exception as e:
                log.error(f"Business unit model validation error: {str(e)}")
                error_detail = ErrorDetail(
                    code="BUSINESS_UNIT_MODEL_ERROR",
                    message="Business unit data validation failed",
                    field="business_unit_data"
                )
                return RestErrors.internal_server_error_500(
                    message="Business unit data validation failed",
                    data=None,
                    errors=[error_detail]
                )

            log.info(f"Business unit retrieved successfully: {bu_id}")

            return RestErrors.success_200(
                message="Business unit retrieved successfully",
                data=bu_data
            )

        except Exception as e:
            log.error(f"Unexpected error during business unit retrieval: {str(e)}")
            error_detail = ErrorDetail(
                code="UNEXPECTED_ERROR",
                message="An unexpected error occurred during business unit retrieval",
                field="system"
            )
            return RestErrors.internal_server_error_500(
                message="An unexpected error occurred during business unit retrieval",
                data=None,
                errors=[error_detail]
            )

    def update_business_unit(self, logged_user: UserProfile, business_unit: BusinessUnit, bu_id: str, org_id: str):
        """
        Update a business unit with comprehensive validation and error handling.
        
        Args:
            logged_user (UserProfile): The authenticated user making the request
            business_unit (BusinessUnit): Updated business unit data
            bu_id (str): The unique identifier for the business unit
            org_id (str): The organization ID where the business unit belongs
            
        Returns:
            dict: Response with success/error status and updated business unit data
        """
        try:
            # Validate logged user's organization
            validation_error = self._validate_logged_user_organization(logged_user)
            if validation_error:
                return validation_error

            # Input validation
            if not bu_id or not bu_id.strip():
                error_detail = ErrorDetail(
                    code="MISSING_BU_ID",
                    message="Business unit ID is required",
                    field="bu_id"
                )
                return RestErrors.bad_request_400(
                    message="Business unit ID is required",
                    data=None,
                    errors=[error_detail]
                )

            if not org_id or not org_id.strip():
                error_detail = ErrorDetail(
                    code="MISSING_ORG_ID",
                    message="Organization ID is required",
                    field="org_id"
                )
                return RestErrors.bad_request_400(
                    message="Organization ID is required",
                    data=None,
                    errors=[error_detail]
                )

            bu_id = bu_id.strip()
            org_id = org_id.strip()
            log.info(f"Updating business unit: {bu_id} in organization: {org_id} by user: {logged_user.user_id}")

            # Check if business unit exists
            try:
                existing_bu = self.mongo_client.find_one(
                    "business_units", 
                    {"bu_id": bu_id, "parent_org": org_id}
                )
            except Exception as e:
                log.error(f"Database error during business unit check: {str(e)}")
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

            if not existing_bu:
                log.warning(f"Business unit not found for update: {bu_id} in organization: {org_id}")
                error_detail = ErrorDetail(
                    code="BUSINESS_UNIT_NOT_FOUND",
                    message="Business unit not found",
                    field="bu_id"
                )
                return RestErrors.not_found_404(
                    message="Business unit not found",
                    data=None,
                    errors=[error_detail]
                )

            # Prepare update data
            update_data = business_unit.dict(exclude_unset=True)
            
            # Remove fields that shouldn't be updated
            protected_fields = ["bu_id", "parent_org", "created_at", "created_by"]
            for field in protected_fields:
                if field in update_data:
                    del update_data[field]

            # Add updated timestamp and user
            update_data["updated_at"] = datetime.utcnow()
            update_data["updated_by"] = logged_user.user_id

            # If name is being updated, check for uniqueness within the organization
            if "name" in update_data and update_data["name"] != existing_bu.get("name"):
                try:
                    existing_name_bu = self.mongo_client.find_one(
                        "business_units", 
                        {
                            "name": update_data["name"], 
                            "parent_org": org_id,
                            "bu_id": {"$ne": bu_id}
                        }
                    )
                except Exception as e:
                    log.error(f"Database error during name check: {str(e)}")
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

                if existing_name_bu:
                    log.warning(f"Business unit name already exists in organization: {update_data['name']}")
                    error_detail = ErrorDetail(
                        code="BU_NAME_ALREADY_EXISTS",
                        message="Business unit name already exists in this organization",
                        field="name"
                    )
                    return RestErrors.bad_request_400(
                        message="Business unit name already exists in this organization",
                        data=None,
                        errors=[error_detail]
                    )

            # Update business unit in database
            try:
                result = self.mongo_client.update_one(
                    "business_units",
                    {"bu_id": bu_id, "parent_org": org_id},
                    {"$set": update_data}
                )
                if result.modified_count == 0:
                    log.warning(f"No changes made to business unit: {bu_id}")
            except Exception as e:
                log.error(f"Database error during business unit update: {str(e)}")
                error_detail = ErrorDetail(
                    code="DATABASE_ERROR",
                    message="Failed to update business unit",
                    field="system"
                )
                return RestErrors.internal_server_error_500(
                    message="Failed to update business unit",
                    data=None,
                    errors=[error_detail]
                )

            # Retrieve updated business unit
            try:
                updated_bu = self.mongo_client.find_one(
                    "business_units", 
                    {"bu_id": bu_id, "parent_org": org_id}
                )
                if "_id" in updated_bu:
                    del updated_bu["_id"]
            except Exception as e:
                log.error(f"Database error during updated business unit retrieval: {str(e)}")
                error_detail = ErrorDetail(
                    code="DATABASE_ERROR",
                    message="Failed to retrieve updated business unit",
                    field="system"
                )
                return RestErrors.internal_server_error_500(
                    message="Failed to retrieve updated business unit",
                    data=None,
                    errors=[error_detail]
                )

            log.info(f"Business unit updated successfully: {bu_id}")

            return RestErrors.success_200(
                message="Business unit updated successfully",
                data=updated_bu
            )

        except Exception as e:
            log.error(f"Unexpected error during business unit update: {str(e)}")
            error_detail = ErrorDetail(
                code="UNEXPECTED_ERROR",
                message="An unexpected error occurred during business unit update",
                field="system"
            )
            return RestErrors.internal_server_error_500(
                message="An unexpected error occurred during business unit update",
                data=None,
                errors=[error_detail]
            )

    def delete_business_unit(self, logged_user: UserProfile, bu_id: str, org_id: str):
        """
        Delete a business unit with comprehensive validation and error handling.
        
        Args:
            logged_user (UserProfile): The authenticated user making the request
            bu_id (str): The unique identifier for the business unit
            org_id (str): The organization ID where the business unit belongs
            
        Returns:
            dict: Response with success/error status
        """
        try:
            # Validate logged user's organization
            validation_error = self._validate_logged_user_organization(logged_user)
            if validation_error:
                return validation_error

            # Input validation
            if not bu_id or not bu_id.strip():
                error_detail = ErrorDetail(
                    code="MISSING_BU_ID",
                    message="Business unit ID is required",
                    field="bu_id"
                )
                return RestErrors.bad_request_400(
                    message="Business unit ID is required",
                    data=None,
                    errors=[error_detail]
                )

            if not org_id or not org_id.strip():
                error_detail = ErrorDetail(
                    code="MISSING_ORG_ID",
                    message="Organization ID is required",
                    field="org_id"
                )
                return RestErrors.bad_request_400(
                    message="Organization ID is required",
                    data=None,
                    errors=[error_detail]
                )

            bu_id = bu_id.strip()
            org_id = org_id.strip()
            log.info(f"Deleting business unit: {bu_id} in organization: {org_id} by user: {logged_user.user_id}")

            # Check if business unit exists
            try:
                existing_bu = self.mongo_client.find_one(
                    "business_units", 
                    {"bu_id": bu_id, "parent_org": org_id}
                )
            except Exception as e:
                log.error(f"Database error during business unit check: {str(e)}")
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

            if not existing_bu:
                log.warning(f"Business unit not found for deletion: {bu_id} in organization: {org_id}")
                error_detail = ErrorDetail(
                    code="BUSINESS_UNIT_NOT_FOUND",
                    message="Business unit not found",
                    field="bu_id"
                )
                return RestErrors.not_found_404(
                    message="Business unit not found",
                    data=None,
                    errors=[error_detail]
                )

            # Check for dependent child business units
            try:
                dependent_bus = self.mongo_client.find_many(
                    "business_units", 
                    {"parent_bu_id": bu_id}
                )
                if dependent_bus and len(list(dependent_bus)) > 0:
                    log.warning(f"Cannot delete business unit with dependent child business units: {bu_id}")
                    error_detail = ErrorDetail(
                        code="BUSINESS_UNIT_HAS_DEPENDENCIES",
                        message="Cannot delete business unit with existing child business units",
                        field="bu_id"
                    )
                    return RestErrors.bad_request_400(
                        message="Cannot delete business unit with existing child business units",
                        data=None,
                        errors=[error_detail]
                    )
            except Exception as e:
                log.error(f"Database error during dependency check: {str(e)}")
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

            # Delete business unit from database
            try:
                result = self.mongo_client.delete_data(
                    "business_units", 
                    {"bu_id": bu_id, "parent_org": org_id}
                )
                if not result:
                    raise Exception("Failed to delete business unit")
            except Exception as e:
                log.error(f"Database error during business unit deletion: {str(e)}")
                error_detail = ErrorDetail(
                    code="DATABASE_ERROR",
                    message="Failed to delete business unit",
                    field="system"
                )
                return RestErrors.internal_server_error_500(
                    message="Failed to delete business unit",
                    data=None,
                    errors=[error_detail]
                )

            # Remove business unit ID from organization's business_units list
            try:
                removal_success = self.remove_business_unit(org_id, [bu_id])
                if not removal_success:
                    log.warning(f"Failed to remove business unit ID {bu_id} from organization {org_id} business_units list")
                else:
                    log.info(f"Successfully removed business unit ID {bu_id} from organization {org_id} business_units list")
            except Exception as e:
                log.warning(f"Error removing business unit ID from organization list: {str(e)}")
                # Note: We don't return an error here as the business unit was successfully deleted

            log.info(f"Business unit deleted successfully: {bu_id}")

            return RestErrors.no_content_204(
                message="Business unit deleted successfully",
                data={
                    'bu_id': bu_id
                },
                errors=[]
            )

        except Exception as e:
            log.error(f"Unexpected error during business unit deletion: {str(e)}")
            error_detail = ErrorDetail(
                code="UNEXPECTED_ERROR",
                message="An unexpected error occurred during business unit deletion",
                field="system"
            )
            return RestErrors.internal_server_error_500(
                message="An unexpected error occurred during business unit deletion",
                data=None,
                errors=[error_detail]
            )

    def get_business_units(self, logged_user: UserProfile, org_id: str, limit: int = 100, skip: int = 0):
        """
        Retrieve all business units within an organization with comprehensive validation, pagination, and error handling.
        
        Args:
            logged_user (UserProfile): The authenticated user making the request
            org_id (str): The organization ID to get business units from
            limit (int): Maximum number of business units to return (default: 100, max: 1000)
            skip (int): Number of business units to skip for pagination (default: 0)
            
        Returns:
            dict: Response with success/error status and list of business unit data
        """
        try:
            # Validate logged user's organization
            validation_error = self._validate_logged_user_organization(logged_user)
            if validation_error:
                return validation_error

            # Input validation
            if not org_id or not org_id.strip():
                error_detail = ErrorDetail(
                    code="MISSING_ORG_ID",
                    message="Organization ID is required",
                    field="org_id"
                )
                return RestErrors.bad_request_400(
                    message="Organization ID is required",
                    data=None,
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

            org_id = org_id.strip()
            log.info(f"Retrieving business units for organization: {org_id}, limit: {limit}, skip: {skip}")

            # Check if parent organization exists
            try:
                parent_org = self.mongo_client.find_one("organizations", {"org_id": org_id})
            except Exception as e:
                log.error(f"Database error during parent organization check: {str(e)}")
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

            if not parent_org:
                log.warning(f"Parent organization not found: {org_id}")
                error_detail = ErrorDetail(
                    code="PARENT_ORGANIZATION_NOT_FOUND",
                    message="Parent organization not found",
                    field="org_id"
                )
                return RestErrors.not_found_404(
                    message="Parent organization not found",
                    data=None,
                    errors=[error_detail]
                )

            # Query business units from database
            try:
                query_filter = {"parent_org": org_id}
                
                # Get total count for pagination metadata
                total_count = self.mongo_client.count_documents("business_units", query_filter)
                
                # Query business units with pagination
                bus_data = self.mongo_client.find_many(
                    "business_units", 
                    query_filter, 
                    limit=limit, 
                    skip=skip,
                    sort=[("created_at", -1)]  # Sort by newest first
                )
                
            except Exception as e:
                log.error(f"Database error during business units retrieval: {str(e)}")
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

            # Process and validate business unit data
            try:
                processed_bus = []
                
                for bu_data in bus_data:
                    # Validate business unit data with BusinessUnit model
                    try:
                        if "_id" in bu_data:
                            del bu_data["_id"]
                        business_unit = BusinessUnit(**bu_data)
                    except Exception as e:
                        log.warning(f"Business unit model validation failed for bu {bu_data.get('bu_id', 'unknown')}: {str(e)}")
                        continue  # Skip invalid business units instead of failing the entire request
                    
                    processed_bus.append(bu_data)
                
            except Exception as e:
                log.error(f"Business unit data processing error: {str(e)}")
                error_detail = ErrorDetail(
                    code="DATA_PROCESSING_ERROR",
                    message="Business unit data processing failed",
                    field="business_unit_data"
                )
                return RestErrors.internal_server_error_500(
                    message="Business unit data processing failed",
                    data=None,
                    errors=[error_detail]
                )

            # Prepare pagination metadata
            pagination_metadata = {
                "total_count": total_count,
                "returned_count": len(processed_bus),
                "limit": limit,
                "skip": skip,
                "has_more": (skip + len(processed_bus)) < total_count
            }

            # Prepare final response data
            response_data = {
                "business_units": processed_bus,
                "pagination": pagination_metadata,
                "organization": {
                    "org_id": org_id,
                    "name": parent_org["name"]
                }
            }

            log.info(f"Business units retrieved successfully: {len(processed_bus)} business units from organization {org_id}")

            return RestErrors.success_200(
                message=f"Business units retrieved successfully. Found {len(processed_bus)} business units.",
                data=response_data
            )

        except Exception as e:
            log.error(f"Unexpected error during business units retrieval: {str(e)}")
            error_detail = ErrorDetail(
                code="UNEXPECTED_ERROR",
                message="An unexpected error occurred during business units retrieval",
                field="system"
            )
            return RestErrors.internal_server_error_500(
                message="An unexpected error occurred during business units retrieval",
                data=None,
                errors=[error_detail]
            )

    def get_organization_units(self, logged_user: UserProfile, org_id: str):
        """
        Retrieve organization data along with detailed information for all its business units.
        
        Args:
            logged_user (UserProfile): The authenticated user making the request
            org_id (str): The organization ID to get units from
            
        Returns:
            dict: Response with success/error status, organization data, and detailed business units data
        """
        try:
            # Validate logged user's organization
            validation_error = self._validate_logged_user_organization(logged_user)
            if validation_error:
                return validation_error

            # Input validation
            if not org_id or not org_id.strip():
                error_detail = ErrorDetail(
                    code="MISSING_ORG_ID",
                    message="Organization ID is required",
                    field="org_id"
                )
                return RestErrors.bad_request_400(
                    message="Organization ID is required",
                    data=None,
                    errors=[error_detail]
                )

            org_id = org_id.strip()
            log.info(f"Retrieving organization units for organization: {org_id} by user: {logged_user.user_id}")

            # Get organization data from database
            try:
                organization_data = self.mongo_client.find_one("organizations", {"org_id": org_id})
            except Exception as e:
                log.error(f"Database error during organization retrieval: {str(e)}")
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

            if not organization_data:
                log.warning(f"Organization not found: {org_id}")
                error_detail = ErrorDetail(
                    code="ORGANIZATION_NOT_FOUND",
                    message="Organization not found",
                    field="org_id"
                )
                return RestErrors.not_found_404(
                    message="Organization not found",
                    data=None,
                    errors=[error_detail]
                )

            # Get business_units list from organization data
            business_unit_ids = organization_data.get("business_units", [])
            
            # Initialize business units data list
            business_units_data = []
            
            # If there are business unit IDs, fetch their detailed data
            if business_unit_ids:
                try:
                    # Fetch all business units data from database
                    business_units_cursor = self.mongo_client.find_many(
                        "business_units", 
                        {"bu_id": {"$in": business_unit_ids}}
                    )
                    
                    # Convert cursor to list and process each business unit
                    for bu_data in business_units_cursor:
                        try:
                            # Validate business unit data with BusinessUnit model
                            validated_bu = BusinessUnit(**bu_data)
                            business_units_data.append(validated_bu.model_dump())
                        except Exception as validation_error:
                            log.warning(f"Business unit validation failed for bu_id {bu_data.get('bu_id', 'unknown')}: {str(validation_error)}")
                            # Include the raw data if validation fails, but log the issue
                            business_units_data.append(bu_data)
                            
                except Exception as e:
                    log.error(f"Database error during business units retrieval: {str(e)}")
                    error_detail = ErrorDetail(
                        code="DATABASE_ERROR",
                        message="Error retrieving business units data",
                        field="business_units"
                    )
                    return RestErrors.internal_server_error_500(
                        message="Error retrieving business units data",
                        data=None,
                        errors=[error_detail]
                    )

            # Check for missing business units (IDs in organization but not found in database)
            found_bu_ids = {bu_data.get("bu_id") for bu_data in business_units_data}
            missing_bu_ids = [bu_id for bu_id in business_unit_ids if bu_id not in found_bu_ids]
            
            if missing_bu_ids:
                log.warning(f"Some business units not found in database: {missing_bu_ids}")

            # Prepare final response data
            pagination_metadata = {
                "total_count": len(business_units_data),
                "returned_count": len(business_units_data),
                "limit": 100,
                "skip": 0,
                "has_more": False
            }

            # Prepare final response data
            response_data = {
                "business_units": business_units_data,
                "pagination": pagination_metadata,
                "organization": {
                    "org_id": organization_data.get("org_id"),
                    "name": organization_data.get("name")
                }
            }

            log.info(f"Organization units retrieved successfully: {len(business_units_data)} business units found for organization {org_id}")

            return RestErrors.success_200(
                message=f"Organization units retrieved successfully. Found {len(business_units_data)} business units.",
                data=response_data
            )

        except Exception as e:
            log.error(f"Unexpected error during organization units retrieval: {str(e)}")
            error_detail = ErrorDetail(
                code="UNEXPECTED_ERROR",
                message="An unexpected error occurred during organization units retrieval",
                field="system"
            )
            return RestErrors.internal_server_error_500(
                message="An unexpected error occurred during organization units retrieval",
                data=None,
                errors=[error_detail]
            )