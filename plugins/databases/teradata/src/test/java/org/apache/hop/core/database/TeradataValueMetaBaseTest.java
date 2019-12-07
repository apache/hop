package org.apache.hop.core.database;

import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.HopLoggingEvent;
import org.apache.hop.core.logging.HopLoggingEventListener;
import org.apache.hop.core.plugins.DatabasePluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.junit.rules.RestoreHopEnvironment;
import org.junit.*;
import org.mockito.Spy;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.doReturn;

public class TeradataValueMetaBaseTest {
    @ClassRule
    public static RestoreHopEnvironment env = new RestoreHopEnvironment();

    // Get PKG from class under test
    private Class<?> PKG = ValueMetaBase.PKG;

    private class StoreLoggingEventListener implements HopLoggingEventListener {

        private List<HopLoggingEvent> events = new ArrayList<>();

        @Override
        public void eventAdded( HopLoggingEvent event ) {
            events.add( event );
        }

        public List<HopLoggingEvent> getEvents() {
            return events;
        }
    }

    private StoreLoggingEventListener listener;


    @Spy
    private DatabaseMeta databaseMetaSpy = spy( new DatabaseMeta() );
    private PreparedStatement preparedStatementMock = mock( PreparedStatement.class );
    private ResultSet resultSet;
    private DatabaseMeta dbMeta;
    private ValueMetaBase valueMetaBase;

    @BeforeClass
    public static void setUpBeforeClass() throws HopException {
        PluginRegistry.addPluginType( ValueMetaPluginType.getInstance() );
        PluginRegistry.addPluginType( DatabasePluginType.getInstance() );
        PluginRegistry.init();
        HopLogStore.init();
    }

    @Before
    public void setUp() {
        listener = new StoreLoggingEventListener();
        HopLogStore.getAppender().addLoggingEventListener( listener );

        valueMetaBase = new ValueMetaBase();
        dbMeta =  new DatabaseMeta( "teradata", "TERADATA", "", "", "", "", "", "" ) ;
        resultSet = mock( ResultSet.class );
    }

    @After
    public void tearDown() {
        HopLogStore.getAppender().removeLoggingEventListener( listener );
        listener = new StoreLoggingEventListener();
    }

    @Ignore
    @Test
    public void testMetdataPreviewSqlDateToPentahoDateUsingTeradata() throws SQLException, HopDatabaseException {
        doReturn( Types.DATE ).when( resultSet ).getInt( "DATA_TYPE" );
        doReturn( mock( TeradataDatabaseMeta.class ) ).when( dbMeta ).getDatabaseInterface();
        System.out.println(dbMeta.getDatabaseInterface());
        ValueMetaInterface valueMeta = valueMetaBase.getMetadataPreview( dbMeta, resultSet );
        assertTrue( valueMeta.isDate() );
        assertEquals( 1, valueMeta.getPrecision() );
    }

}
