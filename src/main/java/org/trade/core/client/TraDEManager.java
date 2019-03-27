package org.trade.core.client;

import io.swagger.trade.client.jersey.*;
import io.swagger.trade.client.jersey.api.ApiClient;
import io.swagger.trade.client.jersey.api.ApiException;
import io.swagger.trade.client.jersey.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;

public class TraDEManager {

    private static final Logger logger = LoggerFactory.getLogger("org.trade.core.client.TraDEManager");

    private final long WAITING_TIME = 1000;
    private final long MAX_RETRY = 120;

    private String[] _simulationId;

    private ApiClient _client;

    private DataModelApi _dataModelApi;
    private DataObjectApi _dataObjectApi;
    private DataObjectInstanceApi _dataObjectInstanceApi;
    private DataElementInstanceApi _dataElementInstApi;
    private DataValueApi _dataValueApi;

    private HashMap<String, String> elementRefToDataElementInstanceIdMap = new HashMap<String, String>();

    public TraDEManager(ApiClient tradeApiClient, String simulationCorrelationKey, String simulationCorrelationValue) {
        _simulationId = new String[]{simulationCorrelationKey, simulationCorrelationValue};

        _client = tradeApiClient;

        _dataModelApi = new DataModelApi(_client);
        _dataObjectApi = new DataObjectApi(_client);
        _dataObjectInstanceApi = new DataObjectInstanceApi(_client);
        _dataElementInstApi = new DataElementInstanceApi(_client);
        _dataValueApi = new DataValueApi(_client);
    }

    /**
     * This method resolves and downloads data of a data value associated to a data element instance based on the
     * provided parameters.
     *
     * @param dataModelNamespace The namespace URL of the data model that defines the underlying data object and data
     *                           element.
     * @param dataModelName      The name of the data model that defines the underlying data object and data
     *                           element.
     * @param dataObjectName     The name of the data object
     * @param dataElementName    The name of the data element
     * @param index              The index of the associated data value to pull, if the data element holds a collection.
     * @return The pulled result data as byte[]
     * @throws Exception the exception
     */
    public byte[] pull(String dataModelNamespace, String dataModelName, String dataObjectName, String
            dataElementName, Integer index)
            throws Exception {
        byte[] result = null;

        if (logger.isDebugEnabled()) {
            logger.debug("TraDE Manager (simulationID: " + _simulationId[1]
                    + ") tries to pull data value for data element "
                    + dataElementName);
        }

        DataElementInstanceWithLinks dEInst = null;
        int retries = 0;

        while ((dEInst == null || dEInst.getInstance() == null)
                && retries < MAX_RETRY) {
            try {
                // Try to resolve a corresponding data element instance
                dEInst = resolveDataElementInstance(dataModelNamespace, dataModelName, dataObjectName, dataElementName);
            } catch (ApiException e) {
                if (e.getCode() == 404) {
                    // Since we were not able to resolve any instance, we assume
                    // that the parent data object is not instantiated at all,
                    // therefore
                    // we have to wait until such an instance is created by a
                    // corresponding data producer.

                    // Wait some time and try again to resolve a data element
                    // instance
                    try {
                        // TODO: Use notifications to avoid polling!
                        Thread.sleep(WAITING_TIME);
                        retries++;
                    } catch (InterruptedException ie) {
                        logger.error("TraDE Manager (simulationID: " + _simulationId[1]
                                + ") pulling data for " + getKey4ElementRef(dataObjectName, dataElementName, index)
                                + " caused an exception.", ie);
                    }
                } else {
                    logger.error("TraDE Manager (simulationID: " + _simulationId[1]
                            + ") pulling data for " + getKey4ElementRef(dataObjectName, dataElementName, index)
                            + " caused an exception.", e);
                }
            }
        }

        // If we were not able to resolve a data element instance after the
        // specified number of retries, we return immediately letting the
        // process instance fault
        if (dEInst != null && dEInst.getInstance() != null) {
            String dataElmInstanceId = dEInst.getInstance().getId();

            // Check if the instance has already an associated data value
            // we can pull, else we have to wait for it again
            DataValueArrayWithLinks valueArray = null;

            while ((valueArray == null || valueArray.getDataValues() == null || valueArray.getDataValues().isEmpty())
                    && retries < MAX_RETRY) {
                try {
                    valueArray = _dataValueApi.getDataValues(dataElmInstanceId, index);
                } catch (ApiException e) {
                    if (e.getCode() == 404) {
                        // Since we were not able to resolve any data value, we
                        // have to wait until such a data value is created by a
                        // corresponding data producer.

                        // Wait some time and try again to resolve a data
                        // element
                        // instance
                        try {
                            // TODO: Find a better way to handle this
                            Thread.sleep(WAITING_TIME);
                            retries++;
                        } catch (InterruptedException ie) {
                            logger.error("TraDE Manager (simulationID: "
                                    + _simulationId[1]
                                    + ") pulling data for " + getKey4ElementRef(dataObjectName, dataElementName, index)
                                    + " caused an exception.", ie);
                        }
                    } else {
                        logger.error("TraDE Manager (simulationID: " + _simulationId[1]
                                + ") pulling data for " + getKey4ElementRef(dataObjectName, dataElementName, index)
                                + " caused an exception.", e);
                    }
                }
            }

            // Pull the data
            if (valueArray != null && valueArray.getDataValues() != null && !valueArray.getDataValues().isEmpty()) {
                DataValueWithLinks value = null;

                if (valueArray.getDataValues().size() > 1) {
                    logger.error("TraDE Manager (simulationID: " + _simulationId[1]
                            + ") pulling data for " + getKey4ElementRef(dataObjectName, dataElementName, index)
                            + " was not successful.");
                } else {
                    // Get the first data value (the correct data value for the provided index is resolved at the
                    // server side)
                    value = valueArray.getDataValues().get(0);
                }

                byte[] data = null;

                if (value != null) {
                    while ((data == null || data.length <= 0)
                            && retries < MAX_RETRY) {
                        try {
                            data = _dataValueApi.pullDataValue(value.getDataValue()
                                    .getId());
                        } catch (ApiException e) {
                            if (e.getCode() == 404) {
                                // Since we were not able to fetch any data, we
                                // have to wait until such a data is uploaded by a
                                // corresponding data producer.

                                // Wait some time and try again to retrieve the data
                                try {
                                    // TODO: Find a better way to handle this
                                    Thread.sleep(WAITING_TIME);
                                    retries++;
                                } catch (InterruptedException ie) {
                                    logger.error("TraDE Manager (simulationID: " + _simulationId[1]
                                            + ") pulling data for " + getKey4ElementRef(dataObjectName, dataElementName, index)
                                            + " caused an exception.", ie);
                                }
                            } else {
                                logger.error("TraDE Manager (simulationID: " + _simulationId[1]
                                        + ") pulling data for " + getKey4ElementRef(dataObjectName, dataElementName, index)
                                        + " caused an exception.", e);
                            }
                        }
                    }
                }

                result = data;
            }
        }

        return result;
    }

    /**
     * This method uploads data to a data value associated to a data element instance based on the
     * provided parameters.
     *
     * @param dataModelNamespace The namespace URL of the data model that defines the underlying data object and data
     *                           element.
     * @param dataModelName      The name of the data model that defines the underlying data object and data
     *                           element.
     * @param dataObjectName     The name of the data object
     * @param dataElementName    The name of the data element
     * @param isCollection       Whether the data element could hold a collection of data or not (i.e., it instances
     *                           could have more than one data value associated)
     * @param data               The data to push as byte[]
     * @param contentType        The content type of the data, i.e., a MIME/Media-Type expressing the format of the data
     * @param index              The index of the associated data value to push to, if the data element holds a
     *                           collection.
     * @return The link to the data value to which the data was pushed (i.e., the data value that holds the data)
     */
    public String push(String dataModelNamespace, String dataModelName, String dataObjectName, String
            dataElementName, boolean isCollection, byte[] data, String contentType, String createdBy, Integer index) {
        String linkToDataValue = "";

        if (logger.isDebugEnabled()) {
            logger.debug("TraDE Manager (simulationID: " + _simulationId[1]
                    + ") tries to push data value for data object # data element: " + getKey4ElementRef(dataObjectName, dataElementName, index));
        }

        // Try to resolve a corresponding data element instance
        DataElementInstanceWithLinks dEInst = null;
        try {
            dEInst = resolveDataElementInstance(dataModelNamespace, dataModelName, dataObjectName, dataElementName);
        } catch (ApiException e) {
            if (e.getCode() == 404) {
                // Nothing to do, since we create a corresponding data element
                // instance in the following
            } else {
                logger.error("TraDE Manager (simulationID: " + _simulationId[1]
                        + ") pushing data from " + getKey4ElementRef(dataObjectName, dataElementName, index)
                        + " caused an exception.", e);
            }
        }

        // Check if we were able to resolve an existing data element
        // instance
        if (dEInst == null || dEInst.getInstance() == null) {
            // Since we were not able to resolve any instance, we assume
            // that
            // the parent data object is not instantiated at all, therefore
            // we
            // create a complete new instance of the data object which also
            // trigger the creation of data element instance.
            DataObjectWithLinks dObj = null;
            try {
                dObj = resolveDataObject(dataModelNamespace, dataModelName, dataObjectName);
            } catch (ApiException e) {
                if (e.getCode() == 404) {
                    // Nothing to do, since we create a corresponding data
                    // object instance in the following
                } else {
                    logger.error("TraDE Manager (simulationID: " + _simulationId[1]
                            + ") pushing data from " + getKey4ElementRef(dataObjectName, dataElementName, index)
                            + " caused an exception.", e);
                }
            }

            if (dObj == null) {
                // TODO: If we also do not find any data object, the question is if we want to create one on the fly?
            } else {
                String dObjId = dObj.getDataObject().getId();

                // Instantiate the referenced data object to which the data
                // element belongs
                DataObjectInstanceWithLinks dObjInstance = null;
                try {
                    dObjInstance = createNewDataObjectInstance(dObjId,
                            createdBy + _simulationId[1]);
                } catch (ApiException e) {
                    logger.error("TraDE Manager (simulationID: " + _simulationId[1]
                            + ") pushing data from " + getKey4ElementRef(dataObjectName, dataElementName, index)
                            + " caused an exception.", e);
                }

                if (dObjInstance != null && dObjInstance.getInstance() != null) {
                    String dObjInstanceId = dObjInstance.getInstance().getId();

                    // Get the correct data element instance from the data
                    // object instance
                    try {
                        dEInst = _dataElementInstApi
                                .getDataElementInstanceByDataElementName(
                                        dObjInstanceId, dataElementName);
                    } catch (ApiException e) {
                        logger.error("TraDE Manager (simulationID: " + _simulationId[1]
                                + ") pushing data from " + getKey4ElementRef(dataObjectName, dataElementName, index)
                                + " caused an exception.", e);
                    }
                }
            }
        }

        if (dEInst != null && dEInst.getInstance() != null) {
            // Register the data element instance Id for later use
            elementRefToDataElementInstanceIdMap.put(getKey4ElementRef(dataObjectName, dataElementName, index), dEInst.getInstance()
                    .getId());

            String dataElmInstanceId = dEInst.getInstance().getId();

            // Check if the instance has already an associated value
            // we have to update, else create a new one
            DataValueArrayWithLinks valueArray = null;

            boolean notFound = false;
            try {
                valueArray = _dataValueApi.getDataValues(dataElmInstanceId, index);
            } catch (ApiException e) {
                if (e.getCode() == 404) {
                    notFound = true;
                } else {
                    logger.error("TraDE Manager (simulationID: " + _simulationId[1]
                            + ") pushing data from " + getKey4ElementRef(dataObjectName, dataElementName, index)
                            + " caused an exception.", e);
                }
            }

            DataValueWithLinks value = null;

            if (notFound) {
                DataValue dataValueData = new DataValue();

                dataValueData.setType("binary");
                dataValueData.setContentType(contentType);

                if (isCollection) {
                    // Add the index to the name if its a collection
                    dataValueData.setName(getKey4ElementRef(dataObjectName, dataElementName, index));
                } else {
                    dataValueData.setName(getKey4ElementRef(dataObjectName, dataElementName, null));
                }
                dataValueData.setCreatedBy(createdBy + _simulationId[1]);

                // Create and associate a new data value to the data element
                // instance
                try {
                    value = _dataValueApi.associateDataValueToDataElementInstance(dEInst.getInstance()
                            .getId(), dataValueData);
                } catch (ApiException e) {
                    logger.error("TraDE Manager (simulationID: " + _simulationId[1]
                            + ") pushing data from " + getKey4ElementRef(dataObjectName, dataElementName, index)
                            + " caused an exception.", e);
                }
            } else {
                value = valueArray.getDataValues().get(0);
            }

            // Push the data
            if (value != null && value.getDataValue() != null) {
                try {
                    _dataValueApi.pushDataValue(value.getDataValue().getId(), data, false,
                            Long.valueOf(data.length));

                    linkToDataValue = value.getDataValue().getHref();
                } catch (ApiException e) {
                    logger.error("TraDE Manager (simulationID: " + _simulationId[1]
                            + ") pushing data from " + getKey4ElementRef(dataObjectName, dataElementName, index)
                            + " caused an exception.", e);
                }
            }
        } else {
            // TODO: What to do if we were not able to resolve an existing nor create a new data element instance?
            // Throw an exception?
        }

        return linkToDataValue;
    }

    private DataObjectInstanceWithLinks createNewDataObjectInstance(
            String dObjId, String createdBy)
            throws ApiException {
        DataObjectInstanceWithLinks result = null;

        // Instantiate the data object
        DataObjectInstanceData instanceData = new DataObjectInstanceData();
        instanceData.setCreatedBy(createdBy);
        instanceData
                .setCorrelationProperties(createCorrelationProperties());

        result = _dataObjectInstanceApi.addDataObjectInstance(dObjId,
                instanceData);

        return result;
    }

    private DataObjectWithLinks resolveDataObject(String dataModelNamespace, String dataModelName, String dataObjectName) throws ApiException {
        DataObjectWithLinks result = null;

        DataModelArrayWithLinks dataModels = _dataModelApi.getDataModels(null,
                null, dataModelNamespace,
                dataModelName, null);

        if (dataModels.getDataModels() != null
                && !dataModels.getDataModels().isEmpty()) {

            Iterator<DataModelWithLinks> iter = dataModels.getDataModels()
                    .iterator();

            while (result == null && iter.hasNext()) {
                DataModelWithLinks model = iter.next();

                // Retrieve the list of data objects which should contain
                // the searched one
                DataObjectArrayWithLinks list = _dataObjectApi.getDataObjects(
                        model.getDataModel().getId(), null, null);
                Iterator<DataObjectWithLinks> iterObj = list.getDataObjects()
                        .iterator();
                while (result == null && iterObj.hasNext()) {
                    DataObjectWithLinks cur = iterObj.next();

                    // Check if the data object is the one we are looking
                    // for
                    if (cur.getDataObject().getName()
                            .equals(dataObjectName)) {
                        result = cur;
                    }
                }
            }
        }

        return result;
    }

    private DataElementInstanceWithLinks resolveDataElementInstance(String dataModelNamespace, String dataModelName, String dataObjectName, String
            dataElementName) throws ApiException {
        DataElementInstanceWithLinks dEInst = null;

        String elementRefKey = getKey4ElementRef(dataObjectName, dataElementName, null);

        // Check if we already know the data element instance from previous
        // push attempts and resolve it
        if (elementRefToDataElementInstanceIdMap.containsKey(elementRefKey)) {
            String dEInstID = elementRefToDataElementInstanceIdMap.get(elementRefKey);

            dEInst = _dataElementInstApi.getDataElementInstance(dEInstID);
        } else {
            // Translate the correlation properties
            CorrelationPropertyArray corrPropArray = createCorrelationProperties();

            // Query the instance of the referenced data
            // element according to the provided correlation properties of
            // the referenced correlation set
            dEInst = _dataElementInstApi.queryDataElementInstance(
                    dataModelNamespace,
                    dataModelName,
                    dataObjectName, dataElementName,
                    corrPropArray);
            if (dEInst != null && dEInst.getInstance() != null) {
                // If there is already an instance we can use, we put its ID
                // to our map to remember it
                elementRefToDataElementInstanceIdMap.put(elementRefKey, dEInst
                        .getInstance().getId());
            }
        }

        return dEInst;
    }

    private String getKey4ElementRef(String dataObjectName, String
            dataElementName, Integer index) {

        String result = dataObjectName + "#" + dataElementName;

        if (index != null) {
            result = result + "#[" + index + "]";
        }

        return result;
    }

    private CorrelationPropertyArray createCorrelationProperties() {

        CorrelationPropertyArray corrPropArray = new CorrelationPropertyArray();

        CorrelationProperty prop = new CorrelationProperty();
        prop.setKey(_simulationId[0]);
        prop.setValue(_simulationId[1]);
        corrPropArray.add(prop);

        return corrPropArray;
    }

    public void cleanup() {
        elementRefToDataElementInstanceIdMap.clear();
        _dataModelApi = null;
        _dataObjectApi = null;
        _dataObjectInstanceApi = null;
        _dataElementInstApi = null;
        _dataValueApi = null;
    }
}
