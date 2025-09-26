import bcrypt
from datetime import datetime, timezone
from scripts.config.application import config

class Commons:

    @staticmethod
    def get_timestamp_in_utc():
        return datetime.now(timezone.utc)

    @staticmethod
    def get_encrypted_password(password: str) -> str:
        """
        Hashes a plain-text password using bcrypt with salt.
        
        Args:
            password: The user's plain-text password.
            
        Returns:
            A string representing the hashed password, including the salt.
        """
        # Get salt rounds from config
        security_config = config.get_security_config()
        salt_rounds = security_config.get('bcrypt_salt_rounds', 12)
        
        # bcrypt automatically generates a salt when you hash the password.
        # The salt is prepended to the final hash.
        hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt(rounds=salt_rounds))
        
        # Return as string instead of bytes
        return hashed_password.decode('utf-8')

    @staticmethod
    def verify_password(plain_password: str, hashed_password: str) -> bool:
        """
        Verifies a plain-text password against a stored hashed password.
        
        Args:
            plain_password: The plain-text password provided by the user during login.
            hashed_password: The hashed password retrieved from the database (as string).
            
        Returns:
            True if the password is correct, False otherwise.
        """
        try:
            # Convert string back to bytes for bcrypt verification
            hashed_bytes = hashed_password.encode('utf-8')
            
            # bcrypt automatically extracts the salt from the hashed password
            # and uses it to hash the provided password for comparison.
            return bcrypt.checkpw(plain_password.encode('utf-8'), hashed_bytes)
        except Exception:
            # Return False if there's any error in verification
            return False
