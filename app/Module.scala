import com.google.inject.AbstractModule
import java.time.Clock

import services._

/**
  * This class is a Guice module that tells Guice how to bind several
  * different types. This Guice module is created when the Play
  * application starts.
  */
class Module extends AbstractModule {

    override def configure() = {
        // Use the system clock as the default implementation of Clock
        bind(classOf[Clock]).toInstance(Clock.systemDefaultZone)

        // Set GpsProjectFacebookStats as the implementation for ProjectStats
        bind(classOf[ProjectStats]).to(classOf[GpsProjectFacebookStats])

        // Set PlainSqlRedshift as DAO implementation for all DB related traits
        bind(classOf[BuildRedshiftQuery]).to(classOf[PlainSqlRedshift])
        bind(classOf[RedshiftTransfer]).to(classOf[PlainSqlRedshift])
        bind(classOf[RedshiftInterpolation]).to(classOf[PlainSqlRedshift])
    }

}
