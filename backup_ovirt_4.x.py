#!/usr/bin/python

import logging
import sys
import time
import ovirtsdk4 as sdk
import ovirtsdk4.types as types

from argparse import ArgumentParser, FileType
from config import Config

"""
Main class to make the backups
"""

logger = logging.getLogger()


def initialize_logger(logger_fmt, logger_file_path, debug):
    logger_options = {
        "format": logger_fmt,
        "level": logging.DEBUG if debug else logging.INFO,
    }
    if logger_file_path:
        logger_options['filename'] = logger_file_path
    logging.basicConfig(**logger_options)


def create_argparser():
    p = ArgumentParser()

    # General options
    p.add_argument(
        "-c", "--config-file",
        help="Path to the config file, pass dash (-) for stdin",
        dest="config_file",
        required=True,
        type=FileType(),
    )
    p.add_argument(
        "-d", "--debug",
        help="Debug flag",
        dest="debug",
        action="store_true",
        default=False,
    )
    p.add_argument(
        "--dry-run",
        help="When set no operation takes effect",
        dest="dry_run",
        action="store_true",
        default=None,  # None because we need to recognize whether it was set.
    )

    osg = p.add_argument_group("oVirt server related options")
    osg.add_argument(
        "--server",
        help="URL to connect to your engine",
        dest="server",
        default=None,
    )
    osg.add_argument(
        "--username",
        help="Username to connect to the engine",
        dest="username",
        default=None,
    )
    osg.add_argument(
        "--password",
        help="Password to connect to the engine",
        dest="password",
        default=None,
    )

    vmg = p.add_argument_group("VM's related arguments")
    vmg.add_argument(
        "-a", "--all-vms",
        help="Backup all VMs and override the list of VM's in the config "
             "file",
        dest="all_vms",
        action="store_true",
        default=False,
    )
    vmg.add_argument(
        "--tag",
        help="define the tag used to override the list of VM's that should"
             " be backed up",
        dest="vm_tag",
        default=False,
    )
    vmg.add_argument(
        "--vm-names",
        help="List of names which VMs should be backed up",
        dest="vm_names",
        default=None,
    )
    vmg.add_argument(
        "--vm-middle",
        help="Middle part for the exported VM name",
        dest="vm_middle",
        default=None,
    )

    dcg = p.add_argument_group("Data Centrum's related options")
    dcg.add_argument(
        "--export-domain",
        help="Name of the NFS Export Domain",
        dest="export_domain",
        default=None,
    )
    dcg.add_argument(
        "--storage-domain",
        help="Storage domain where VMs are located",
        dest="storage_domain",
        default=None,
    )
    dcg.add_argument(
        "--cluster-name",
        help="Name of the cluster where VMs should be cloned",
        dest="cluster_name",
        default=None,
    )
    mscg = p.add_argument_group("Miscellaneous options")
    mscg.add_argument(
        "--snapshot-description",
        help="Description which should be set to created snapshot",
        dest="snapshot_description",
        default=None,
    )
    mscg.add_argument(
        "--timeout",
        help="Timeout in seconds to wait for time consuming operation",
        dest="timeout",
        default=None,
    )
    mscg.add_argument(
        "--backup-keep-count",
        help="Number of days to keep backups",
        dest="backup_keep_count",
        default=None,
    )
    mscg.add_argument(
        "--vm-name-max-length",
        help="Limit for length of VM's name ",
        dest="vm_name_max_length",
        default=None,
    )
    mscg.add_argument(
        "--use-short-suffix",
        help="If set it will use short suffix for VM's name",
        dest="use_short_suffix",
        action="store_true",
        default=None,
    )
    mscg.add_argument(
        "--storage-space-threshold",
        help="The number in interval (0, 1), to free space on storage domain.",
        dest="storage_space_threshold",
        type=float,
        default=None,
    )
    mscg.add_argument(
        "--persist-memorystate",
        help="If set, the VM is being paused during snapshot creation.",
        dest="persist_memorystate",
        action="store_true",
        default=None,
    )

    lg = p.add_argument_group("Logging related options")
    lg.add_argument(
        "--logger-fmt",
        help="This value is used to format log messages",
        dest="logger_fmt",
        default=None,
    )
    lg.add_argument(
        "--logger-file-path",
        help="Path to file where we to store log messages",
        dest="logger_file_path",
        default=None,
    )
    return p


def arguments_to_dict(opts):
    result = {}
    ignored_keys = ('config_file', 'dry_run', 'debug')

    for key, val in vars(opts).items():
        if key in ignored_keys:
            continue  # These doesn't have a place in config file
        if val is not None:
            result[key] = val

    return result


def main(argv):
    p = create_argparser()
    opts = p.parse_args(argv)
    config_arguments = arguments_to_dict(opts)

    global config
    with opts.config_file:
        config = Config(opts.config_file, opts.debug, config_arguments)
    initialize_logger(
        config.get_logger_fmt(), config.get_logger_file_path(), opts.debug,
    )

    time_start = int(time.time())

    has_errors = False

    try:
        # Connect to server
        connection = connect()

        # Get a reference to the root service:
        system_service = connection.system_service()

        # Get the reference to the "vms" service:
        vms_service = system_service.vms_service()

        # Get the reference to the storage domains service:
        sds_service = system_service.storage_domains_service()

        # Get the reference to the disk service:
        disks_service = system_service.disks_service()

        # Get the reference to the service that we will use to send events to the audit log:
        events_service = system_service.events_service()

        # Add all VM's to the config file
        if opts.all_vms:
            vms = vms_service.list(max=400)
            config.set_vm_names([vm.name for vm in vms])
            # Update config file
            if opts.config_file.name != "<stdin>":
                config.write_update(opts.config_file.name)

        # Add VM's with the tag to the vm list
        if opts.vm_tag:
            vms = vms_service.list(max=400, query="tag=" + opts.vm_tag)
            config.set_vm_names([vm.name for vm in vms])
            # Update config file
            if opts.config_file.name != "<stdin>":
                config.write_update(opts.config_file.name)

        # Test if config export_domain is valid
        if not sds_service.list(search="%s" % config.get_export_domain()):
            logger.error("!!! Check the export_domain in the config")
            connection.close()
            sys.exit(1)

        # Test if config storage_domain is valid
        if not sds_service.list(search="%s" % config.get_storage_domain()):
            logger.error("!!! Check the storage_domain in the config")
            connection.close()
            sys.exit(1)

        # Test if all VM names are valid
        for vm_from_list in config.get_vm_names():
            if not vms_service.list(search="name=%s" % str(vm_from_list), all_content=True):
                logger.error("!!! There are no VM with the following name in your cluster: %s", vm_from_list)
                connection.close()
                sys.exit(1)

        # Test if config vm_middle is valid
        if not config.get_vm_middle():
            logger.error("!!! It's not valid to leave vm_middle empty")
            connection.close()
            sys.exit(1)

        vms_with_failures = list(config.get_vm_names())

        for vm_from_list in config.get_vm_names():
            config.clear_vm_suffix()
            vm_clone_name = vm_from_list + config.get_vm_middle() + config.get_vm_suffix()

            # Check VM name length limitation
            length = len(vm_clone_name)
            if length > config.get_vm_name_max_length():
                logger.error("!!! VM name with middle and suffix are to long (size: %s, allowed %s) !!!", length,
                             config.get_vm_name_max_length())
                logger.info("VM name: %s", vm_clone_name)
                connection.close()
                sys.exit(1)

            logger.info("Start backup for: %s", vm_from_list)

            try:
                # Get the VM
                vms = vms_service.list(search="name=%s" % str(vm_from_list))
                if not vms:
                    logger.warn(
                        "The VM (%s) doesn't exist anymore, skipping backup ...",
                        vm_from_list
                    )
                    continue

                # Getting VM by UUID
                vm_service = vms_service.vm_service(vms[0].id)

                # Locate the service that manages the snapshots of the virtual machine:
                snapshots_service = vm_service.snapshots_service()

                # Cleanup: Delete the cloned VM
                # remove_snapshots( snapshots_service )

                # Create a VM snapshot:
                snapshot = create_snapshot(vms[0], snapshots_service, events_service)

                # Clone snapshot to a VM
                clone_snapshot_to_vm(config.get_cluster_name(), vm_clone_name, vms_service, snapshot)

                # Export cloned VM to Export_Domain
                export_cloned_vm(config.get_export_domain(), vm_clone_name, vms_service)

                # Cleanup: Delete the snapshot
                remove_snapshot(snapshot, snapshots_service)

                # Cleanup: Delete the cloned VM
                remove_cloned_vm(vm_clone_name, vms_service)

                # vms_with_failures.remove( vm_name )

            except sdk.ConnectionError as e:
                logger.error("!!! Can't connect to the server %s", e)
                connection = connect();
                continue
            except sdk.Error as e:
                logger.error("!!! Got a Error: %s", e)
                has_errors = True
                continue
            except Exception as e:
                logger.error("!!! Got unexpected exception: %s", e)
                connection.close()
                sys.exit(1)

        logger.info("All backups done")

        if has_errors:
            logger.info("Some errors occured during the backup, please check the log file")
            connection.close()
            sys.exit(1)

    finally:
        # Disconnect from the server
        connection.close()


def create_snapshot(vms, snapshots_service, events_service):
    logger.info("Snapshot creation started ...")

    try:
        events_service.add(
            event=types.Event(
                vm=types.Vm(
                    id=vms.id,
                ),
                origin="Backup_of_VM",
                severity=types.LogSeverity.NORMAL,
                custom_id=int(time.time()),
                description=(
                        'Backup of virtual machine \'%s\' using snapshot \'%s\' is '
                        'starting.' % (vms.name, config.get_snapshot_description())
                ),
            ),
        )

        # Add the new snapshot:
        snapshot = snapshots_service.add(
            types.Snapshot(
                description=config.get_snapshot_description(),
                persist_memorystate=False,
            ),
        )

        # 'Waiting for Snapshot creation to finish'
        snapshot_service = snapshots_service.snapshot_service(snapshot.id)

        while True:
            time.sleep(5)
            snapshot = snapshot_service.get()
            if snapshot.snapshot_status == types.SnapshotStatus.OK:
                break

        logger.info("Snapshot created")
    except Exception as e:
        logger.info("Can't create snapshot for VM: %s", vms.name)
        logger.info("DEBUG: %s", e)

    return snapshot


def remove_snapshot(snapshot, snapshots_service):
    logger.info("Removing old snapshot started ...")

    try:
        snapshot_service = snapshots_service.snapshot_service(snapshot.id)
        disks_service = snapshot_service.disks_service()

        if disks_service.list():
            snapshot_service.remove()
    except Exception as e:
        logger.info("DEBUG: %s", e)

    logger.info("Snapshot removed")


def remove_snapshots(snapshots_service):
    logger.info("Removing old snapshots started ...")

    try:
        for snap in snapshots_service.list():
            snapshot_service = snapshots_service.snapshot_service(snap.id)
            disks_service = snapshot_service.disks_service()

            if disks_service.list():
                snapshot = snapshot_service.get()
                if snapshot.snapshot_status == types.SnapshotStatus.LOCKED:
                    continue

                snapshot_service.remove()

                while True:
                    time.sleep(5)
                    snapshot = snapshot_service.get()
                    if not disks_service.list():
                        break

    except Exception as e:
        logger.info("DEBUG: %s", e)

    logger.info("Snapshots removed")


def clone_snapshot_to_vm(cluster, vm_clone_name, vms_service, snapshot):
    logger.info("Clone into VM (%s) started ..." % vm_clone_name)

    try:
        cloned_vm = vms_service.add(
            vm=types.Vm(
                name=vm_clone_name,
                snapshots=[
                    types.Snapshot(
                        id=snapshot.id
                    )
                ],
                cluster=types.Cluster(
                    name=cluster
                )
            )
        )

        cloned_vm_service = vms_service.vm_service(cloned_vm.id)

        while True:
            time.sleep(5)
            cloned_vm = cloned_vm_service.get()
            if cloned_vm.status == types.VmStatus.DOWN:
                break
    except Exception as e:
        logger.info("Can't clone snapshot to VM: %s", vm_clone_name)
        logger.info("DEBUG: %s", e)

    logger.info("Snapshot cloned ...")


def export_cloned_vm(export_domain, vm_clone_name, vms_service):
    logger.info("Exporting cloned (%s) VM started ..." % vm_clone_name)

    try:
        vms = vms_service.list(search="name=%s" % str(vm_clone_name))
        if not vms:
            logger.warning(
                "The VM (%s) doesn't exist anymore, skipping backup ...",
                vm_clone_name
            )
            raise Exception

        vm_service = vms_service.vm_service(vms[0].id)

        vm_service.export(
            exclusive=True,
            discard_snapshots=True,
            storage_domain=types.StorageDomain(
                name=export_domain
            )
        )

        while True:
            time.sleep(15)
            vms = vm_service.get()
            if vms.status == types.VmStatus.DOWN:
                break

    except Exception as e:
        logger.info("Can't export cloned (%s) VM", vm_clone_name)
        logger.info("DEBUG: %s", e)

    logger.info("Cloned VM exported successfully")


def remove_cloned_vm(vm_clone_name, vms_service):
    logger.info("Removing cloned (%s) VM started ..." % vm_clone_name)

    try:
        vms = vms_service.list(search="name=%s" % str(vm_clone_name))
        if not vms:
            logger.warning(
                "The VM (%s) doesn't exist anymore, skipping backup ...",
                vm_clone_name
            )
            raise Exception

        vm_service = vms_service.vm_service(vms[0].id)
        vm_service.remove()

    except Exception as e:
        logger.info("Can't remove cloned (%s) VM", vm_clone_name)
        logger.info("DEBUG: %s", e)

    logger.info("Cloned VM removed")


def connect():
    return sdk.Connection(
        url=config.get_server(),
        username=config.get_username(),
        password=config.get_password(),
        insecure=True,
        debug=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])
