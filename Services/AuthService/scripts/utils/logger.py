import logging
import logging.handlers
import os
import sys
import inspect
from typing import Optional

# Import the configuration
from scripts.config.application import config

class Logger:
    _instance: Optional['Logger'] = None
    _logger: Optional[logging.Logger] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Logger, cls).__new__(cls)
            cls._instance._setup_logger()
        return cls._instance
    
    def _setup_logger(self):
        """Setup logger with configuration from application.yaml"""
        if self._logger is not None:
            return
        
        # Get logging configuration
        log_config = config.get_logging_config()
        
        # Create logger
        self._logger = logging.getLogger('UptimeReporting')
        
        # Set logging level
        level = getattr(logging, log_config.get('level', 'INFO').upper())
        self._logger.setLevel(level)
        
        # Clear existing handlers
        self._logger.handlers.clear()
        
        # Create formatter
        formatter = logging.Formatter(
            fmt=log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(funcName)s:%(lineno)d] - %(message)s'),
            datefmt=log_config.get('date_format', '%Y-%m-%d %H:%M:%S')
        )
        
        # Setup console handler
        console_config = log_config.get('handlers', {}).get('console', {})
        if console_config.get('enabled', True):
            console_handler = logging.StreamHandler(sys.stdout)
            console_level = getattr(logging, console_config.get('level', 'DEBUG').upper())
            console_handler.setLevel(console_level)
            console_handler.setFormatter(formatter)
            self._logger.addHandler(console_handler)
        
        # Setup file handler
        file_config = log_config.get('handlers', {}).get('file', {})
        if file_config.get('enabled', False):
            log_file = file_config.get('filename', 'logs/application.log')
            
            # Create logs directory if it doesn't exist
            log_dir = os.path.dirname(log_file)
            if log_dir and not os.path.exists(log_dir):
                os.makedirs(log_dir, exist_ok=True)
            
            # Create rotating file handler
            file_handler = logging.handlers.RotatingFileHandler(
                log_file,
                maxBytes=file_config.get('max_bytes', 10485760),  # 10MB
                backupCount=file_config.get('backup_count', 5)
            )
            
            file_level = getattr(logging, file_config.get('level', 'INFO').upper())
            file_handler.setLevel(file_level)
            file_handler.setFormatter(formatter)
            self._logger.addHandler(file_handler)
    
    def _log_with_caller_info(self, level: int, message: str, *args, **kwargs):
        """Log message with caller information from stack inspection"""
        # Get the caller's frame (2 levels up: _log_with_caller_info -> debug/info/etc -> actual caller)
        frame = inspect.currentframe()
        try:
            caller_frame = frame.f_back.f_back
            if caller_frame:
                # Extract caller information
                filename = os.path.basename(caller_frame.f_code.co_filename)
                func_name = caller_frame.f_code.co_name
                line_no = caller_frame.f_lineno
                
                # Create a custom LogRecord with caller information
                record = self._logger.makeRecord(
                    name=self._logger.name,
                    level=level,
                    fn=caller_frame.f_code.co_filename,
                    lno=line_no,
                    msg=message,
                    args=args,
                    exc_info=kwargs.get('exc_info'),
                    func=func_name,
                    extra=kwargs.get('extra'),
                    sinfo=kwargs.get('stack_info')
                )
                
                # Override the filename to show just the basename
                record.filename = filename
                
                # Handle the record
                self._logger.handle(record)
            else:
                # Fallback to regular logging if frame inspection fails
                self._logger.log(level, message, *args, **kwargs)
        finally:
            del frame
    
    def debug(self, message: str, *args, **kwargs):
        """Log debug message"""
        self._log_with_caller_info(logging.DEBUG, message, *args, **kwargs)
    
    def info(self, message: str, *args, **kwargs):
        """Log info message"""
        self._log_with_caller_info(logging.INFO, message, *args, **kwargs)
    
    def warning(self, message: str, *args, **kwargs):
        """Log warning message"""
        self._log_with_caller_info(logging.WARNING, message, *args, **kwargs)
    
    def warn(self, message: str, *args, **kwargs):
        """Alias for warning"""
        self.warning(message, *args, **kwargs)
    
    def error(self, message: str, *args, **kwargs):
        """Log error message"""
        self._log_with_caller_info(logging.ERROR, message, *args, **kwargs)
    
    def critical(self, message: str, *args, **kwargs):
        """Log critical message"""
        self._log_with_caller_info(logging.CRITICAL, message, *args, **kwargs)
    
    def exception(self, message: str, *args, **kwargs):
        """Log exception with traceback"""
        # For exceptions, we want to include the exception info
        kwargs['exc_info'] = True
        self._log_with_caller_info(logging.ERROR, message, *args, **kwargs)
    
    def set_level(self, level: str):
        """Set logging level dynamically"""
        log_level = getattr(logging, level.upper())
        self._logger.setLevel(log_level)

# Create global logger instance
log = Logger()