#!/usr/bin/env python
"""
This module provides business object class to interact with Primary Dataset. 
"""

__revision__ = "$Id: DBSPrimaryDataset.py,v 1.2 2009/10/27 17:24:47 akhukhun Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.DAOFactory import DAOFactory

class DBSPrimaryDataset:
    """
    Primary Dataset business object class
    """
    def __init__(self, logger, dbi):
        self.daofactory = DAOFactory(package='dbs.dao', logger=logger, dbinterface=dbi)
        self.logger = logger
        self.dbi = dbi

    def listPrimaryDatasets(self, primdsname=""):
        """
        Returns all primary datasets if primdsname is not passed.
        """
        primdslist = self.daofactory(classname="PrimaryDataset.List")
        result = primdslist.execute(primdsname)
        return result

    def insertPrimaryDataset(self, businput):
        """
        Input dictionary has to have the following keys:
        primarydsname, primarydstype, creationdate, createby
        it builds the correct dictionary for dao input and executes the dao
        """
        primdstplist = self.daofactory(classname="PrimaryDSType.List")
        primdsinsert = self.daofactory(classname="PrimaryDataset.Insert")
        seqmanager = self.daofactory(classname="SequenceManager")

        primdsname = businput["primarydsname"]
        primdstype = businput["primarydstype"]
        primdsObj = businput
        primdsObj.pop("primarydstype")
        conn = self.dbi.connection()
        tran = conn.begin()
        try:
            primarydstypeslist = primdstplist.execute(primdstype, conn, True)
            assert len(primarydstypeslist) == 1, "PrimaryDSType %s does not exist" % (primdstype)
            primdstypeid = primarydstypeslist[0]["primary_ds_type_id"]
            primdsid = seqmanager.increment("SEQ_PDS", conn, True)
            primdsObj.update({"primarydstypeid":primdstypeid, "primarydsid":primdsid})
            primdsinsert.execute(primdsObj, conn, True)
            tran.commit()
        except Exception, e:
            tran.rollback()
            self.logger.exception(e)
            raise
        finally:
            conn.close()
