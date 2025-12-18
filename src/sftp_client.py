"""
SFTP connection manager for accessing SAS files.
Uses paramiko for file operations.
"""

import logging

import paramiko
from keboola.component.exceptions import UserException

from configuration import SftpConnection


class FileMetadata:
    """Metadata for a file on SFTP server."""

    def __init__(self, filename: str, mtime: float, size: int):
        self.filename = filename
        self.mtime = mtime  # Last modification timestamp
        self.size = size  # File size in bytes


class SftpClient:
    """
    SFTP client for SAS file extraction.

    Uses paramiko for file listing and downloading.
    """

    def __init__(self, config: SftpConnection):
        """
        Initialize SFTP client.

        Args:
            config: SFTP connection configuration
        """
        self.config = config
        self.ssh_client: paramiko.SSHClient | None = None
        self.sftp_client: paramiko.SFTPClient | None = None
        self._connected = False

    def connect(self):
        """
        Establish SFTP connections.

        Creates paramiko connection for listing and downloading.

        Raises:
            UserException: If connection fails
        """
        try:
            # 1. Create SSH client with paramiko
            self.ssh_client = paramiko.SSHClient()
            self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            logging.info(f"Connecting to SFTP server {self.config.host}:{self.config.port}")

            self.ssh_client.connect(
                hostname=self.config.host,
                port=self.config.port,
                username=self.config.username,
                password=self.config.password,
                timeout=30,
            )

            # 2. Open SFTP channel
            self.sftp_client = self.ssh_client.open_sftp()
            logging.info("SFTP connection established")

            # Optimization for large file transfers
            transport = self.ssh_client.get_transport()
            if transport:
                # Increasing window size can significantly speed up SFTP transfers
                # by allowing more data "in flight" before an acknowledgment is required.
                transport.window_size = 2147483647
                transport.packetizer.REKEY_BYTES = 2147483647
                transport.packetizer.REKEY_PACKETS = 2147483647

            self._connected = True

        except paramiko.AuthenticationException as e:
            raise UserException(f"SFTP authentication failed: {e}")
        except paramiko.SSHException as e:
            raise UserException(f"SFTP connection error: {e}")
        except Exception as e:
            raise UserException(f"Failed to connect to SFTP server: {e}")

    def list_sas_files(self) -> list[str]:
        """
        List all .sas7bdat files in the configured folder.

        Returns:
            Sorted list of SAS file names

        Raises:
            UserException: If listing fails or not connected
        """
        if not self._connected or self.sftp_client is None:
            raise UserException("Not connected to SFTP server. Call connect() first.")

        try:
            logging.info(f"Listing SAS files in {self.config.folder_path}")

            # List directory contents
            all_files = self.sftp_client.listdir(self.config.folder_path)

            # Filter for .sas7bdat files only
            sas_files = [f for f in all_files if f.endswith(".sas7bdat")]

            # Sort alphabetically
            sas_files.sort()

            logging.info(f"Found {len(sas_files)} SAS files")
            return sas_files

        except FileNotFoundError:
            raise UserException(f"SFTP folder not found: {self.config.folder_path}")
        except PermissionError:
            raise UserException(f"Permission denied accessing: {self.config.folder_path}")
        except Exception as e:
            raise UserException(f"Failed to list files from SFTP: {e}")

    def download_file(self, remote_path: str, local_path: str):
        """
        Download a file from SFTP using optimized streaming and prefetching.
        Much faster than standard sftp.get() for large files.

        Args:
            remote_path: Path on SFTP server
            local_path: Destination path on local filesystem
        """
        if not self._connected or self.sftp_client is None:
            raise UserException("Not connected to SFTP server.")

        logging.info(f"Downloading {remote_path} using optimized stream")
        try:
            with self.sftp_client.open(remote_path, "rb") as remote_file:
                # Start prefetching chunks in the background
                remote_file.prefetch()

                with open(local_path, "wb") as local_file:
                    # Use a large buffer size for copying
                    # 1MB chunks are generally efficient for network/disk I/O crossover
                    buffer_size = 1024 * 1024
                    while True:
                        data = remote_file.read(buffer_size)
                        if not data:
                            break
                        local_file.write(data)

        except Exception as e:
            raise UserException(f"Failed to download file {remote_path}: {e}")

    def get_file_metadata(self, filename: str) -> FileMetadata:
        """
        Get metadata for a specific file.

        Args:
            filename: Name of the file (not full path)

        Returns:
            FileMetadata object with file information

        Raises:
            UserException: If metadata retrieval fails
        """
        if not self._connected or self.sftp_client is None:
            raise UserException("Not connected to SFTP server. Call connect() first.")

        try:
            full_path = f"{self.config.folder_path}/{filename}"
            attrs = self.sftp_client.stat(full_path)

            return FileMetadata(
                filename=filename,
                mtime=attrs.st_mtime if attrs.st_mtime else 0,
                size=attrs.st_size if attrs.st_size else 0,
            )

        except FileNotFoundError:
            raise UserException(f"File not found on SFTP server: {filename}")
        except Exception as e:
            raise UserException(f"Failed to get file metadata for {filename}: {e}")

    def get_sftp_url(self, filename: str) -> str:
        """
        Construct SFTP URL for DuckDB to read.

        Args:
            filename: Name of the file (not full path)

        Returns:
            SFTP URL (e.g., 'sftp:///path/to/file.sas7bdat')
        """
        full_path = f"{self.config.folder_path}/{filename}"
        return f"sftp://{full_path}"

    def close(self):
        """
        Close all SFTP connections.
        """
        if self.sftp_client:
            try:
                self.sftp_client.close()
                logging.debug("SFTP client closed")
            except Exception as e:
                logging.warning(f"Error closing SFTP client: {e}")

        if self.ssh_client:
            try:
                self.ssh_client.close()
                logging.debug("SSH client closed")
            except Exception as e:
                logging.warning(f"Error closing SSH client: {e}")

        self._connected = False
        logging.info("SFTP connections closed")
