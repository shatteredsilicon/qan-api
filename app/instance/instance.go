/*
   Copyright (c) 2016, Percona LLC and/or its affiliates. All rights reserved.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU Affero General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU Affero General Public License for more details.

   You should have received a copy of the GNU Affero General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>
*/

package instance

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/shatteredsilicon/qan-api/app/db"
	"github.com/shatteredsilicon/qan-api/app/db/mysql"
	"github.com/shatteredsilicon/ssm/proto"
)

const SSMServerName = "ssm-server"

type DbHandler interface {
	Create(in proto.Instance) (uint, error)
	Get(uuid string) (uint, *proto.Instance, error)
	Update(in proto.Instance) error
	Delete(uuid string) error
	DeleteData(uuid string) error
	GetAll(regardInternalData bool) ([]proto.Instance, error)
}

// --------------------------------------------------------------------------

func GetInstanceId(db *sql.DB, uuid string) (uint, error) {
	if uuid == "" {
		return 0, nil
	}
	var instanceId uint
	err := db.QueryRow("SELECT instance_id FROM instances WHERE uuid = ?", uuid).Scan(&instanceId)
	if err != nil {
		return 0, mysql.Error(err, "SELECT instances")
	}
	return instanceId, nil
}

func GetInstanceIds(db *sql.DB, uuids []string) ([]uint, error) {
	var instanceIds []uint

	if len(uuids) == 0 {
		return []uint{}, nil
	}

	placeholders := "?" + strings.Repeat(",?", len(uuids)-1)
	values := make([]interface{}, len(uuids))
	for i := range uuids {
		values[i] = uuids[i]
	}
	rows, err := db.Query(fmt.Sprintf("SELECT instance_id FROM instances WHERE uuid IN (%s)", placeholders), values...)
	if err != nil {
		return []uint{}, mysql.Error(err, "SELECT instances")
	}
	defer rows.Close()

	var instanceId uint
	for rows.Next() {
		err = rows.Scan(&instanceId)
		if err != nil {
			return []uint{}, mysql.Error(err, "SELECT instances")
		}
		instanceIds = append(instanceIds, instanceId)
	}

	return instanceIds, nil
}

// --------------------------------------------------------------------------

type MySQLHandler struct {
	dbm db.Manager
}

func NewMySQLHandler(dbm db.Manager) *MySQLHandler {
	n := &MySQLHandler{
		dbm: dbm,
	}
	return n
}

func (h *MySQLHandler) Create(in proto.Instance) (uint, error) {
	if in.ParentUUID != "" {
		id, err := GetInstanceId(h.dbm.DB(), in.ParentUUID)
		if err != nil {
			return 0, fmt.Errorf("Error while checking parent uuid: %v", err)
		}
		if id == 0 {
			return 0, fmt.Errorf("invalid parent uuid %s", in.ParentUUID)
		}
	}

	var dsn interface{}
	if in.DSN != "" {
		dsn = in.DSN
	}

	// todo: validate higher up
	subsys, err := GetSubsystemByName(in.Subsystem)
	if err != nil {
		return 0, err
	}

	columns := []string{"subsystem_id", "parent_uuid", "uuid", "dsn", "name", "distro", "version"}
	args := []interface{}{subsys.Id, in.ParentUUID, in.UUID, dsn, in.Name, in.Distro, in.Version}
	if !in.Deleted.IsZero() {
		columns = append(columns, "deleted")
		args = append(args, in.Deleted)
	}

	res, err := h.dbm.DB().Exec(
		fmt.Sprintf("INSERT INTO instances (%s) VALUES (?%s)", strings.Join(columns, ", "), strings.Repeat(", ?", len(columns)-1)), args...)
	if err != nil {
		return 0, mysql.Error(err, "MySQLHandlerCreate INSERT instances")
	}

	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("cannot get instance last insert id")
	}

	return uint(id), nil
}

func (h *MySQLHandler) Get(uuid string) (uint, *proto.Instance, error) {
	query := "SELECT subsystem_id, instance_id, parent_uuid, uuid, dsn, name, distro, version, created, deleted" +
		" FROM instances" +
		" WHERE uuid = ?"
	return h.getInstance(query, uuid)
}

func (h *MySQLHandler) GetByName(subsystem, name, parentUUID string) (uint, *proto.Instance, error) {
	s, err := GetSubsystemByName(subsystem)
	if err != nil {
		return 0, nil, err
	}

	query := "SELECT subsystem_id, instance_id, parent_uuid, uuid, dsn, name,  distro, version, created, deleted" +
		" FROM instances" +
		" WHERE subsystem_id = ? AND name = ?"

	if parentUUID != "" {
		query = query + " AND parent_uuid = ?"
		return h.getInstance(query, s.Id, name, parentUUID)
	}

	return h.getInstance(query, s.Id, name)
}

func (h *MySQLHandler) GetAll(regardInternalData bool) ([]proto.Instance, error) {
	query := "(SELECT subsystem_id, instance_id, parent_uuid, uuid, dsn, name, distro, version, created, deleted" +
		" FROM instances " +
		" WHERE deleted IS NULL OR YEAR(deleted)=1970)"
	if regardInternalData {
		query += fmt.Sprintf(" UNION (SELECT i.subsystem_id, i.instance_id, i.parent_uuid, i.uuid, i.dsn, i.name, i.distro, i.version, i.created, i.deleted"+
			" FROM instances i"+
			" JOIN query_class_metrics qcm ON i.instance_id = qcm.instance_id"+
			" WHERE i.name = '%s' AND i.subsystem_id = %d"+
			" LIMIT 1)", SSMServerName, SubsystemMySQL)
	}
	query += " ORDER BY name"

	rows, err := h.dbm.DB().Query(query)
	if err != nil {
		return nil, mysql.Error(err, "MySQLHandler.GetAll SELECT instances")
	}
	defer rows.Close()

	internalAdded := false
	instances := []proto.Instance{}
	for rows.Next() {
		in := proto.Instance{}
		var instanceId, subsystemId uint
		var dsn, parentUUID, distro, version sql.NullString
		var deleted mysqlDriver.NullTime
		err = rows.Scan(
			&subsystemId,
			&instanceId,
			&parentUUID,
			&in.UUID,
			&dsn,
			&in.Name,
			&distro,
			&version,
			&in.Created,
			&deleted,
		)
		if err != nil {
			return nil, err
		}

		if in.Name == SSMServerName && in.Subsystem == SubsystemNameMySQL && internalAdded {
			continue
		}

		in.ParentUUID = parentUUID.String
		in.DSN = dsn.String
		in.Distro = distro.String
		in.Version = version.String
		in.Deleted = deleted.Time
		subsystem, err := GetSubsystemById(subsystemId) // todo: cache
		if err != nil {
			return nil, err
		}
		in.Subsystem = subsystem.Name

		instances = append(instances, in)
		if in.Name == SSMServerName && in.Subsystem == SubsystemNameMySQL {
			internalAdded = true
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return instances, nil
}

func (h *MySQLHandler) getInstance(query string, params ...interface{}) (uint, *proto.Instance, error) {
	in := &proto.Instance{}

	var instanceId, subsystemId uint
	var dsn, parentUUID, distro, version sql.NullString
	var deleted mysqlDriver.NullTime

	err := h.dbm.DB().QueryRow(query, params...).Scan(
		&subsystemId,
		&instanceId,
		&parentUUID,
		&in.UUID,
		&dsn,
		&in.Name,
		&distro,
		&version,
		&in.Created,
		&deleted,
	)
	if err != nil {
		return 0, nil, mysql.Error(err, "MySQLHandler.Get SELECT instances")
	}

	in.ParentUUID = parentUUID.String
	in.DSN = dsn.String
	in.Distro = distro.String
	in.Version = version.String
	in.Deleted = deleted.Time
	subsystem, err := GetSubsystemById(subsystemId)
	if err != nil {
		return 0, nil, err
	}
	in.Subsystem = subsystem.Name

	return instanceId, in, nil
}

func (h *MySQLHandler) Update(in proto.Instance) error {
	if in.ParentUUID != "" {
		id, err := GetInstanceId(h.dbm.DB(), in.ParentUUID)
		if err != nil {
			return fmt.Errorf("Error while checking parent uuid: %v", err)
		}
		if id == 0 {
			return fmt.Errorf("invalid parent uuid %s", in.ParentUUID)
		}
	}

	// If deleted was NULL in db, go turns it into “0001-01-01 00:00:00 +0000 UTC" and MySQL doesn’t like it.
	if in.Deleted.IsZero() {
		in.Deleted = time.Unix(1, 0)
	}
	_, err := h.dbm.DB().Exec(
		"UPDATE instances SET parent_uuid = ?, dsn = ?, name = ?, distro = ?, version = ?, deleted = ? WHERE uuid = ?",
		in.ParentUUID, in.DSN, in.Name, in.Distro, in.Version, in.Deleted, in.UUID)
	if err != nil {
		return mysql.Error(err, "MySQLHandler.Update UPDATE instances")
	}

	// todo: return error if no rows affected

	return nil
}

func (h *MySQLHandler) Delete(uuid string) error {
	_, err := h.dbm.DB().Exec("UPDATE instances SET deleted = NOW() WHERE uuid = ?", uuid)
	return mysql.Error(err, "MySQLHandler.Delete UPDATE instances")
}

func (h *MySQLHandler) DeleteData(uuid string) error {
	// clear query_class_metrics table
	_, err := h.dbm.DB().Exec(`
DELETE qcm
FROM query_class_metrics qcm
JOIN instances i ON qcm.instance_id = i.instance_id
WHERE i.uuid = ?
`, uuid)
	if err != nil {
		return mysql.Error(err, "MySQLHandler.DeleteData DELETE query_class_metrics")
	}

	// clear query_examples table
	_, err = h.dbm.DB().Exec(`
DELETE qe
FROM query_examples qe
JOIN instances i ON qe.instance_id = i.instance_id
WHERE i.uuid = ?
`, uuid)
	if err != nil {
		return mysql.Error(err, "MySQLHandler.DeleteData DELETE query_examples")
	}

	// clear query_global_metrics table
	_, err = h.dbm.DB().Exec(`
DELETE qgm
FROM query_global_metrics qgm
JOIN instances i ON qgm.instance_id = i.instance_id
WHERE i.uuid = ?
`, uuid)
	if err != nil {
		return mysql.Error(err, "MySQLHandler.DeleteData DELETE query_global_metrics")
	}

	// clear agent_configs table data
	_, err = h.dbm.DB().Exec(`
DELETE ac
FROM agent_configs ac
JOIN instances i ON ac.other_instance_id = i.instance_id
WHERE i.uuid = ?
`, uuid)
	if err != nil {
		return mysql.Error(err, "MySQLHandler.DeleteData DELETE agent_configs")
	}

	return nil
}
